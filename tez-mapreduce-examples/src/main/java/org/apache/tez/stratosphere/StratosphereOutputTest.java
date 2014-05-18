package org.apache.tez.stratosphere;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
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
import org.apache.tez.runtime.api.TezOutputContext;
import org.apache.tez.runtime.api.Writer;
import org.apache.tez.runtime.api.events.DataMovementEvent;
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
public class StratosphereOutputTest extends AbstractLogicalOutput {

    private static final Log LOG = LogFactory.getLog(StratosphereOutputTest.class);

    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    protected Configuration conf;
    //private int numPhysicalOutputs;
    // Do we need the checksum stuff here?
    private FSDataOutputStream out;
    private FileBasedTupleWriter writer;

    private boolean dataViaEventsEnabled;
    private int dataViaEventsMaxSize;

    public StratosphereOutputTest(){};

    @Override
    public List<Event> initialize() throws Exception {

        this.conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
        this.conf.setStrings(TezJobConfig.LOCAL_DIRS,
                getContext().getWorkDirs());
        //this.numPhysicalOutputs = getNumPhysicalOutputs();

        getContext().requestInitialMemory(0l, null); // mandatory call FROM ONFileUnorderedKVOutput

        this.dataViaEventsEnabled = conf.getBoolean(
                TezJobConfig.TEZ_RUNTIME_BROADCAST_DATA_VIA_EVENTS_ENABLED,
                TezJobConfig.TEZ_RUNTIME_BROADCAST_DATA_VIA_EVENTS_ENABLED_DEFAULT);
        this.dataViaEventsMaxSize = conf.getInt(
                TezJobConfig.TEZ_RUNTIME_BROADCAST_DATA_VIA_EVENTS_MAX_SIZE,
                TezJobConfig.TEZ_RUNTIME_BROADCAST_DATA_VIA_EVENTS_MAX_SIZE_DEFAULT);

        this.writer = new FileBasedTupleWriter(getContext(), conf);
        return Collections.emptyList();

    }

    @Override
    public void start() throws Exception {
        isStarted.set(true);
        //this.writer = new FileBasedTupleWriter(outputContext, conf);
    }

    @Override
    public Writer getWriter() throws Exception {
        Preconditions.checkState(isStarted.get(), "Cannot get writer before starting the Output");
        return this.writer;
    }

    @Override
    public void handleEvents(List<Event> outputEvents) {
        throw new TezUncheckedException("Not expecting any events");
    }

    @Override
    public List<Event> close() throws Exception {
        boolean outputGenerated = this.writer.close();

        ShuffleUserPayloads.DataMovementEventPayloadProto.Builder payloadBuilder = ShuffleUserPayloads.DataMovementEventPayloadProto
                .newBuilder();

        LOG.info("Closing KVOutput: RawLength: " + this.writer.getRawLength()
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

    @VisibleForTesting
    @InterfaceAudience.Private
    String getHost() {
        return System.getenv(ApplicationConstants.Environment.NM_HOST.toString());
    }
}
