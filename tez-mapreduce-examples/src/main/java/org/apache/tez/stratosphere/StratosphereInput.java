package org.apache.tez.stratosphere;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteArrayDataInput;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.base.StringSerializer;
import eu.stratosphere.api.java.io.TextInputFormat;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.io.InputSplit;
import eu.stratosphere.nephele.services.iomanager.Deserializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReaderTez;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.mapreduce.hadoop.InputSplitInfo;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfToTezTranslator;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.RootInputDataInformationEvent;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import eu.stratosphere.api.common.io.InputFormat;

/**
 * Created by filip on 19.05.14.
 */
public class StratosphereInput<T> extends AbstractLogicalInput {

    private static final Log LOG = LogFactory.getLog(StratosphereInput.class);

    private final Lock rrLock = new ReentrantLock();
    private Condition rrInited = rrLock.newCondition();

    private volatile boolean eventReceived = false;

    private JobConf jobConf;
    private Configuration incrementalConf;
    private boolean readerCreated = false;

    org.apache.hadoop.mapreduce.TaskAttemptContext taskAttemptContext;
    protected TextInputFormat inputFormat;
    protected TypeSerializer<T> serializer;
    protected StratosphereReader<T> recordReader;
    protected FileInputSplit inputSplit;

    protected JobSplit.TaskSplitIndex splitMetaInfo = new JobSplit.TaskSplitIndex();

    private TezCounter inputRecordCounter;
    // Potential counters - #splits, #totalSize, #actualyBytesRead

    @InterfaceAudience.Private
    volatile boolean splitInfoViaEvents;

    /**
     * Helper API to generate the user payload for the MRInput and
     * MRInputAMSplitGenerator (if used). The InputFormat will be invoked by Tez
     * at DAG runtime to generate the input splits.
     *
     * @param conf
     *          Configuration for the InputFormat
     * @param inputFormatClassName
     *          Name of the class of the InputFormat
     * @return returns the user payload to be set on the InputDescriptor of  MRInput
     * @throws java.io.IOException
     */
    public static byte[] createUserPayload(Configuration conf, String inputFormatClassName)
            throws IOException {
        Configuration inputConf = new JobConf(conf);
        inputConf.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR,
                inputFormatClassName);
        MultiStageMRConfToTezTranslator.translateVertexConfToTez(inputConf, null);
        StratosphereHelpers.doJobClientMagic(inputConf);
        return StratosphereHelpers.createMRInputPayload(inputConf);
    }

    @Override
    public List<Event> initialize() throws IOException {
        getContext().requestInitialMemory(0l, null); //mandatory call
        getContext().inputIsReady();
        MRRuntimeProtos.MRInputUserPayloadProto mrUserPayload =
                StratosphereHelpers.parseMRInputPayload(getContext().getUserPayload());
        Preconditions.checkArgument(mrUserPayload.hasSplits() == false,
                "Split information not expected in StratosphereInput");
        Configuration conf = StratosphereHelpers.createConfFromByteString(mrUserPayload.getConfigurationBytes());
        this.jobConf = new JobConf(conf);
        // Add tokens to the jobConf - in case they are accessed within the RR / IF
        jobConf.getCredentials().mergeAll(UserGroupInformation.getCurrentUser().getCredentials());

        TaskAttemptID taskAttemptId = new TaskAttemptID(
                new TaskID(
                        Long.toString(getContext().getApplicationId().getClusterTimestamp()),
                        getContext().getApplicationId().getId(), TaskType.MAP,
                        getContext().getTaskIndex()),
                getContext().getTaskAttemptNumber());

        jobConf.set(MRJobConfig.TASK_ATTEMPT_ID,
                taskAttemptId.toString());
        jobConf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID,
                getContext().getDAGAttemptNumber());

        this.inputRecordCounter = getContext().getCounters().findCounter(TaskCounter.INPUT_RECORDS_PROCESSED);

        this.splitInfoViaEvents = jobConf.getBoolean(MRJobConfig.MR_TEZ_SPLITS_VIA_EVENTS,
                MRJobConfig.MR_TEZ_SPLITS_VIA_EVENTS_DEFAULT);

        initializeInternal();
        return null;
    }

    @Override
    public void start() {
    }

    @InterfaceAudience.Private
    void initializeInternal() throws IOException {
        // Primarily for visibility
        rrLock.lock();
        try {
            if (splitInfoViaEvents) {
                setupInputFormat();
            } else {
                // Read split information.
                JobSplit.TaskSplitMetaInfo[] allMetaInfo = readSplits(jobConf);
                JobSplit.TaskSplitMetaInfo thisTaskMetaInfo = allMetaInfo[getContext()
                        .getTaskIndex()];
                this.splitMetaInfo = new JobSplit.TaskSplitIndex(
                        thisTaskMetaInfo.getSplitLocation(),
                        thisTaskMetaInfo.getStartOffset());
                setupInputFormat();
                inputSplit = (FileInputSplit)getSplitDetailsFromDisk(splitMetaInfo);
                setupRecordReader();
            }
        } finally {
            rrLock.unlock();
        }
        LOG.info("Initialzed StratosphereInput: " + getContext().getSourceVertexName());
    }

    private void setupInputFormat() throws IOException {
        this.inputFormat = new TextInputFormat(new eu.stratosphere.core.fs.Path(
                this.jobConf.get(StratosphereHelpers.CONF_INPUT_FILE)));
        this.inputFormat.configure(new eu.stratosphere.configuration.Configuration());
        this.serializer = (TypeSerializer<T>)new StringSerializer();
    }

    private void setupRecordReader() throws IOException {
        Preconditions.checkNotNull(inputSplit, "Input split hasn't yet been setup");
        recordReader = new StratosphereInputReader<T>(serializer);
    }

    @Override
    public StratosphereInputReader<T> getReader() throws IOException {
        Preconditions
                .checkState(readerCreated == false,
                        "Only a single instance of record reader can be created for this input.");
        readerCreated = true;
        rrLock.lock();
        try {
            if (recordReader == null)
                checkAndAwaitRecordReaderInitialization();
        } finally {
            rrLock.unlock();
        }

        LOG.info("Creating reader for StratosphereInput: "
                + getContext().getSourceVertexName());
        return new StratosphereInputReader<T>(serializer);
    }

    @Override
    public void handleEvents(List<Event> inputEvents) throws Exception {
        if (eventReceived || inputEvents.size() != 1) {
            throw new IllegalStateException(
                    "MRInput expects only a single input. Received: current eventListSize: "
                            + inputEvents.size() + "Received previous input: "
                            + eventReceived);
        }
        Event event = inputEvents.iterator().next();
        Preconditions.checkArgument(event instanceof RootInputDataInformationEvent,
                getClass().getSimpleName()
                        + " can only handle a single event of type: "
                        + RootInputDataInformationEvent.class.getSimpleName()
        );

        processSplitEvent((RootInputDataInformationEvent)event);
    }

    @Override
    public List<Event> close() throws IOException {
        return null;
    }

    /**
     * {@link StratosphereInput} sets some additional parameters like split location when using
     * the new API. This methods returns the list of additional updates, and
     * should be used by Processors using the old MapReduce API with {@link StratosphereInput}.
     *
     * @return the additional fields set by {@link StratosphereInput}
     */
    public Configuration getConfigUpdates() {
        if (incrementalConf != null) {
            return new Configuration(incrementalConf);
        }
        return null;
    }

    private org.apache.hadoop.mapreduce.TaskAttemptContext createTaskAttemptContext() {
        return new org.apache.tez.mapreduce.hadoop.mapreduce.TaskAttemptContextImpl(this.jobConf, getContext(), true, null);
    }

    void processSplitEvent(RootInputDataInformationEvent event)
            throws IOException {
        rrLock.lock();
        try {
            initFromEventInternal(event);
            LOG.info("Notifying on RecordReader Initialized");
            rrInited.signal();
        } finally {
            rrLock.unlock();
        }
    }

    void checkAndAwaitRecordReaderInitialization() throws IOException {
        try {
            LOG.info("Awaiting RecordReader initialization");
            rrInited.await();
        } catch (Exception e) {
            throw new IOException(
                    "Interrupted waiting for RecordReader initiailization");
        }
    }

    private void initFromEventInternal(RootInputDataInformationEvent initEvent)
            throws IOException {
        LOG.info("Initializing RecordReader from event");
        Preconditions.checkState(initEvent != null, "InitEvent must be specified");
        inputSplit = (FileInputSplit)getSplitDetailsFromEvent(initEvent.getUserPayload(), jobConf);
        //eu.stratosphere.configuration.Configuration strConf = new eu.stratosphere.configuration.Configuration();
        //strConf.set
        ((TextInputFormat)inputFormat).setCharsetName("UTF-8");
        inputFormat.open(inputSplit);
        LOG.info("Split Details -> SplitClass: "
            + inputSplit.getClass().getName() + ", InputSplit: " + inputSplit);
        setupRecordReader();
        LOG.info("Initialized RecordReader from event");
    }

    @InterfaceAudience.Private
    public static InputSplit getSplitDetailsFromEvent(
            byte[] payload, Configuration conf) throws IOException {
        Preconditions.checkNotNull(payload, "splitProto must be specified");

        SerializationFactory serializationFactory = new SerializationFactory(conf);
        FileInputSplit inputSplit = new FileInputSplit();
        DataInputBuffer buffer = new DataInputBuffer();
        buffer.reset(payload, payload.length);
        inputSplit.read(buffer);
        return inputSplit;
    }

    @SuppressWarnings("unchecked")
    private InputSplit getSplitDetailsFromDisk(JobSplit.TaskSplitIndex splitMetaInfo) throws IOException {
        // TODO
        Path file = new Path(splitMetaInfo.getSplitLocation());
        long offset = splitMetaInfo.getStartOffset();

        // Split information read from local filesystem.
        FileSystem fs = FileSystem.getLocal(jobConf);
        file = fs.makeQualified(file);
        LOG.info("Reading input split file from : " + file);
        InputSplit split = new FileInputSplit(1, new eu.stratosphere.core.fs.Path(file.toUri()), offset, 1024, null);
        return split;
    }

    private void setIncrementalConfigParams(InputSplit inputSplit) {
        throw new RuntimeException("Method not implemented yet!");
        /*if (inputSplit instanceof FileSplit) {
            FileSplit fileSplit = (FileSplit) inputSplit;
            this.incrementalConf = new Configuration(false);

            this.incrementalConf.set(JobContext.MAP_INPUT_FILE, fileSplit.getPath()
                    .toString());
            this.incrementalConf.setLong(JobContext.MAP_INPUT_START,
                    fileSplit.getStart());
            this.incrementalConf.setLong(JobContext.MAP_INPUT_PATH,
                    fileSplit.getLength());
        }
        LOG.info("Processing split: " + inputSplit);*/
    }

    protected JobSplit.TaskSplitMetaInfo[] readSplits(Configuration conf)
            throws IOException {
        JobSplit.TaskSplitMetaInfo[] allTaskSplitMetaInfo;
        allTaskSplitMetaInfo = SplitMetaInfoReaderTez.readSplitMetaInfo(conf,
                FileSystem.getLocal(conf));
        return allTaskSplitMetaInfo;
    }

    private class StratosphereInputReader<T> implements StratosphereReader<T> {

        private TypeSerializer<T> serializer;
        StratosphereInputReader(TypeSerializer<T> serializer) {
            this.serializer = serializer;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean hasNext() throws IOException {
            return !inputFormat.reachedEnd();
        }

        @Override
        public T getNext() throws Exception{
            T reuse = serializer.createInstance();
            reuse = ((InputFormat<T, ?>)inputFormat).nextRecord(reuse);
            return reuse;
        }
    }
}
