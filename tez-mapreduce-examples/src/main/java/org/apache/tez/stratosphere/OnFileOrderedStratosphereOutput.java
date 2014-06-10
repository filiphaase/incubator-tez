package org.apache.tez.stratosphere;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.api.common.typeutils.base.IntSerializer;
import eu.stratosphere.api.common.typeutils.base.StringComparator;
import eu.stratosphere.api.common.typeutils.base.StringSerializer;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.runtime.RuntimeStatefulSerializerFactory;
import eu.stratosphere.api.java.typeutils.runtime.RuntimeStatelessSerializerFactory;
import eu.stratosphere.api.java.typeutils.runtime.TupleComparator;
import eu.stratosphere.api.java.typeutils.runtime.TupleSerializer;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.util.MutableObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.sort.impl.ExternalSorter;
import org.apache.tez.runtime.library.shuffle.common.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test class for using stratosphere serializer as output.
 * Mainly based on Tez-class OnFileUnorderedKVOutput.java
 */
public class OnFileOrderedStratosphereOutput<T> extends AbstractLogicalOutput {

    private static final Log LOG = LogFactory.getLog(OnFileOrderedStratosphereOutput.class);

    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    protected Configuration conf;
    // private int numPhysicalOutputs;
    // Do we need the checksum stuff here?
    private FSDataOutputStream out;
    private FileBasedTupleWriter<T> writer;
    protected MemoryUpdateCallbackHandler memoryUpdateCallbackHandler;
    protected ExternalSorter sorter;

    private boolean dataViaEventsEnabled;
    private int dataViaEventsMaxSize;
    private TypeSerializer<T> serializer;

    public OnFileOrderedStratosphereOutput(){};

    @Override
    public List<Event> initialize() throws Exception {

        this.conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
        this.conf.setStrings(TezJobConfig.LOCAL_DIRS,
                getContext().getWorkDirs());

        this.dataViaEventsEnabled = conf.getBoolean(
                TezJobConfig.TEZ_RUNTIME_BROADCAST_DATA_VIA_EVENTS_ENABLED,
                TezJobConfig.TEZ_RUNTIME_BROADCAST_DATA_VIA_EVENTS_ENABLED_DEFAULT);
        this.dataViaEventsMaxSize = conf.getInt(
                TezJobConfig.TEZ_RUNTIME_BROADCAST_DATA_VIA_EVENTS_MAX_SIZE,
                TezJobConfig.TEZ_RUNTIME_BROADCAST_DATA_VIA_EVENTS_MAX_SIZE_DEFAULT);

        // We need to give this by constructor
        this.serializer = (TypeSerializer<T>) new TupleSerializer(Tuple2.class, new TypeSerializer[] {
                new StringSerializer(),
                new IntSerializer()
        });


        this.conf.setStrings(TezJobConfig.LOCAL_DIRS, getContext().getWorkDirs());
        this.memoryUpdateCallbackHandler = new MemoryUpdateCallbackHandler();
        getContext().requestInitialMemory(
                ExternalSorter.getInitialMemoryRequirement(conf,
                        getContext().getTotalMemoryAvailableToTask()), memoryUpdateCallbackHandler);

        this.writer = new FileBasedTupleWriter(getContext(), conf, serializer);
        return Collections.emptyList();
    }

    @Override
    public void start() throws Exception {
        isStarted.set(true);
    }

    @Override
    public StratosphereWriter<T> getWriter() throws Exception {
        LOG.info("getWriter +++");
        Preconditions.checkState(isStarted.get(), "Cannot get writer before starting the Output");
        return this.writer;
    }

    @Override
    public void handleEvents(List<Event> outputEvents) {
        throw new TezUncheckedException("Not expecting any events");
    }

    @Override
    public List<Event> close() throws Exception {

        LOG.info("In close() of OrderedStratopshere Output +++");
        this.writer.close();
        MutableObjectIterator<T> unsortedIter = this.writer.getDataIterator();

        final int MEMORY_SIZE = 1024 * 1024 * 78;
        TypeSerializerFactory serFactory = null;
        if(serializer.isStateful()){
            serFactory = new RuntimeStatefulSerializerFactory(serializer, Tuple2.class);
        }else{
            serFactory = new RuntimeStatelessSerializerFactory(serializer, Tuple2.class);
        }
        TypeComparator<T> comparator = new TupleComparator(new int[]{0},
                new TypeComparator[]{new StringComparator(true)},
                new TypeSerializer[]{new StringSerializer()});
        AbstractTask parentTask = new DummyInvokable();
        IOManager ioManager = new IOManager();
        MemoryManager memoryManager = new DefaultMemoryManager(MEMORY_SIZE);

        LOG.info("Setting up UnilateralSortMerger");
        UnilateralSortMerger<T> merger = new UnilateralSortMerger(memoryManager, ioManager, unsortedIter, parentTask,
                serFactory, comparator, MEMORY_SIZE, 2, 0.9f);
        LOG.info("Setup");

        // Get the sorted data from stratosphere Sort and write it into a new TEZ writer for now
        MutableObjectIterator<T> sortedIter = merger.getIterator();
        T reuse = serializer.createInstance();
        FileBasedTupleWriter<T> sortedWriter = new FileBasedTupleWriter(getContext(), conf, serializer);
        this.writer = sortedWriter;
        while((reuse = sortedIter.next(reuse)) != null){
            writer.write(reuse);
        }

        // TEZ CODE
        boolean outputGenerated = this.writer.close();

        ShuffleUserPayloads.DataMovementEventPayloadProto.Builder payloadBuilder = ShuffleUserPayloads.DataMovementEventPayloadProto
                .newBuilder();

        LOG.info("Closing Stratosphere-Tuple-Output: RawLength: " + this.writer.getRawLength()
                + ", CompressedLength: " + this.writer.getCompressedLength());

        if (dataViaEventsEnabled && outputGenerated && this.writer.getCompressedLength() <= dataViaEventsMaxSize) {
            LOG.info("Serialzing actual data into DataMovementEvent, dataSize: " + this.writer.getCompressedLength());
            byte[] data = this.writer.getData();
            ShuffleUserPayloads.DataProto.Builder dataProtoBuilder = ShuffleUserPayloads.DataProto.newBuilder();
            dataProtoBuilder.setData(ByteString.copyFrom(data));
            dataProtoBuilder.setRawLength((int)this.writer.getRawLength());
            dataProtoBuilder.setCompressedLength((int)this.writer.getCompressedLength());
            payloadBuilder.setData(dataProtoBuilder.build());
        }

        String host = getHost();
        ByteBuffer shuffleMetadata = getContext()
                .getServiceProviderMetaData(ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID);
        int shufflePort = ShuffleUtils
                .deserializeShuffleProviderMetaData(shuffleMetadata);
        // Set the list of empty partitions - single partition on this case.
        if (!outputGenerated) {
            LOG.info("No output was generated");
            BitSet emptyPartitions = new BitSet();
            emptyPartitions.set(0);
            ByteString emptyPartitionsBytesString =
                    TezUtils.compressByteArrayToByteString(TezUtils.toByteArray(emptyPartitions));
            payloadBuilder.setEmptyPartitions(emptyPartitionsBytesString);
        }
        if (outputGenerated) {
            payloadBuilder.setHost(host);
            payloadBuilder.setPort(shufflePort);
            payloadBuilder.setPathComponent(getContext().getUniqueIdentifier());
        }
        ShuffleUserPayloads.DataMovementEventPayloadProto payloadProto = payloadBuilder.build();

        DataMovementEvent dmEvent = new DataMovementEvent(0,
                payloadProto.toByteArray());
        List<Event> events = Lists.newArrayListWithCapacity(1);
        events.add(dmEvent);
        return events;
    }

    @InterfaceAudience.Private
    String getHost() {
        return System.getenv(ApplicationConstants.Environment.NM_HOST.toString());
    }

    public class DummyInvokable extends AbstractTask
    {

        @Override
        public void registerInputOutput() {}

        @Override
        public void invoke() throws Exception {}
    }
}
