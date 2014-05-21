package org.apache.tez.stratosphere;

import eu.stratosphere.api.java.io.TextOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfToTezTranslator;
import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by filip on 21.05.14.
 */
public class StratosphereOutput<T> extends AbstractLogicalOutput {

    private static final Log LOG = LogFactory.getLog(StratosphereOutput.class);

    private final NumberFormat taskNumberFormat = NumberFormat.getInstance();
    private final NumberFormat nonTaskNumberFormat = NumberFormat.getInstance();

    private JobConf jobConf;
    boolean useNewApi;
    private AtomicBoolean closed = new AtomicBoolean(false);

    eu.stratosphere.api.common.io.OutputFormat outputFormat;
    StratosphereWriter<T> writer;

    private TezCounter outputRecordCounter;


    /**
     * Creates the user payload to be set on the OutputDescriptor for MROutput
     * @param conf Configuration for the OutputFormat
     * @param outputFormatName Name of the class of the OutputFormat
     * @param useNewApi Use new mapreduce API or old mapred API
     * @return
     * @throws java.io.IOException
     */
    public static byte[] createUserPayload(Configuration conf,
                                           String outputFormatName, boolean useNewApi) throws IOException {
        Configuration outputConf = new JobConf(conf);
        outputConf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, outputFormatName);
        outputConf.setBoolean("mapred.mapper.new-api", useNewApi);
        MultiStageMRConfToTezTranslator.translateVertexConfToTez(outputConf,
                null);
        MRHelpers.doJobClientMagic(outputConf);
        return TezUtils.createUserPayloadFromConf(outputConf);
    }

    @Override
    public List<Event> initialize() throws IOException, InterruptedException {
        LOG.info("Initializing Simple Output");
        getContext().requestInitialMemory(0l, null); //mandatory call
        taskNumberFormat.setMinimumIntegerDigits(5);
        taskNumberFormat.setGroupingUsed(false);
        nonTaskNumberFormat.setMinimumIntegerDigits(3);
        nonTaskNumberFormat.setGroupingUsed(false);
        Configuration conf = TezUtils.createConfFromUserPayload(
                getContext().getUserPayload());
        this.jobConf = new JobConf(conf);
        // Add tokens to the jobConf - in case they are accessed within the RW / OF
        jobConf.getCredentials().mergeAll(UserGroupInformation.getCurrentUser().getCredentials());
        this.useNewApi = this.jobConf.getUseNewMapper();
        jobConf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID,
                getContext().getDAGAttemptNumber());
        TaskAttemptID taskAttemptId = org.apache.tez.mapreduce.hadoop.mapreduce.TaskAttemptContextImpl
                .createMockTaskAttemptID(getContext(), false); // WAS ISMAP
        jobConf.set(JobContext.TASK_ATTEMPT_ID, taskAttemptId.toString());
        jobConf.set(JobContext.TASK_ID, taskAttemptId.getTaskID().toString());
        jobConf.setBoolean(JobContext.TASK_ISMAP, false); // WAS ISMAP
        jobConf.setInt(JobContext.TASK_PARTITION,
                taskAttemptId.getTaskID().getId());
        jobConf.set(JobContext.ID, taskAttemptId.getJobID().toString());

        outputRecordCounter = getContext().getCounters().findCounter(TaskCounter.OUTPUT_RECORDS);

        outputFormat = new TextOutputFormat(new eu.stratosphere.core.fs.Path(jobConf.get(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR)));
        outputFormat.configure(new eu.stratosphere.configuration.Configuration());
        outputFormat.open(1,1); // TODO

        LOG.info("Initialized Simple Output"
                + ", using_new_api: " + useNewApi);
        return null;
    }

    @Override
    public void start() {
    }

    private String getOutputFileNamePrefix() {
        String prefix = jobConf.get(MRJobConfig.MROUTPUT_FILE_NAME_PREFIX);
        if (prefix == null) {
            prefix = "part-v" +
                    nonTaskNumberFormat.format(getContext().getTaskVertexIndex()) +
                    "-o" + nonTaskNumberFormat.format(getContext().getOutputIndex());
        }
        return prefix;
    }

    private String getOutputName() {
        // give a unique prefix to the output name
        return getOutputFileNamePrefix() +
                "-" + taskNumberFormat.format(getContext().getTaskIndex());
    }

    @Override
    public StratosphereWriter<T> getWriter() throws IOException {
        this.writer = new StratosphereOutputWriter<T>();
        return writer;
    }

    @Override
    public void handleEvents(List<Event> outputEvents) {
        // Not expecting any events at the moment.
    }

    @Override
    public synchronized List<Event> close() throws IOException {
        if (closed.getAndSet(true)) {
            return null;
        }

        outputFormat.close();
        LOG.info("Closed Simple Output");
        return null;
    }

    private class StratosphereOutputWriter<T> implements StratosphereWriter<T>{

        @Override
        public void write(T element) throws IOException {
            outputFormat.writeRecord(element);
        }
    }
}
