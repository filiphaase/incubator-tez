package org.apache.tez.stratosphere;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.io.InputSplit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.mapreduce.hadoop.InputSplitInfoMem;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TezRootInputInitializer;
import org.apache.tez.runtime.api.TezRootInputInitializerContext;
import org.apache.tez.runtime.api.events.RootInputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.RootInputDataInformationEvent;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * Created by filip on 19.05.14.
 */
public class StratosphereInputAMSplitGenerator implements TezRootInputInitializer  {

        private boolean sendSerializedEvents;

        private static final Log LOG = LogFactory
                .getLog(StratosphereInputAMSplitGenerator.class);

        public StratosphereInputAMSplitGenerator() {
        }

        @Override
        public List<Event> initialize(TezRootInputInitializerContext rootInputContext)
                throws Exception {
            Stopwatch sw = null;
            if (LOG.isDebugEnabled()) {
                sw = new Stopwatch().start();
            }
            MRRuntimeProtos.MRInputUserPayloadProto userPayloadProto = MRHelpers
                    .parseMRInputPayload(rootInputContext.getUserPayload());
            if (LOG.isDebugEnabled()) {
                sw.stop();
                LOG.debug("Time to parse MRInput payload into prot: "
                        + sw.elapsedMillis());
            }
            if (LOG.isDebugEnabled()) {
                sw.reset().start();
            }
            Configuration conf = MRHelpers.createConfFromByteString(userPayloadProto
                    .getConfigurationBytes());

            sendSerializedEvents = conf.getBoolean(
                    MRJobConfig.MR_TEZ_INPUT_INITIALIZER_SERIALIZE_EVENT_PAYLOAD,
                    MRJobConfig.MR_TEZ_INPUT_INITIALIZER_SERIALIZE_EVENT_PAYLOAD_DEFAULT);
            LOG.info("Emitting serialized splits: " + sendSerializedEvents);
            if (LOG.isDebugEnabled()) {
                sw.stop();
                LOG.debug("Time converting ByteString to configuration: " + sw.elapsedMillis());
            }

            if (LOG.isDebugEnabled()) {
                sw.reset().start();
            }

            int totalResource = rootInputContext.getTotalAvailableResource().getMemory();
            int taskResource = rootInputContext.getVertexTaskResource().getMemory();
            float waves = conf.getFloat(
                    TezConfiguration.TEZ_AM_GROUPING_SPLIT_WAVES,
                    TezConfiguration.TEZ_AM_GROUPING_SPLIT_WAVES_DEFAULT);

            int numTasks = (int)((totalResource*waves)/taskResource);

            LOG.info("Input " + rootInputContext.getInputName() + " asking for " + numTasks
                    + " tasks. Headroom: " + totalResource + " Task Resource: "
                    + taskResource + " waves: " + waves);

            // Read all credentials into the credentials instance stored in JobConf.
            JobConf jobConf = new JobConf(conf);
            jobConf.getCredentials().mergeAll(UserGroupInformation.getCurrentUser().getCredentials());

            String realInputFormatName = userPayloadProto.getInputFormatName();
            LOG.info("Grouping stratosphere input splits");

            Job job = Job.getInstance(jobConf);
            FileInputSplit[] splits = StratosphereHelpers.generateNewSplits(job, realInputFormatName, numTasks);

            // Move all this into a function
            List<VertexLocationHint.TaskLocationHint> locationHints = Lists
                .newArrayListWithCapacity(splits.length);

            for(FileInputSplit split : splits) {
                if(split.getHostNames() != null) {
                    locationHints.add(new VertexLocationHint.TaskLocationHint(new HashSet<String>(Arrays
                            .asList(split.getHostNames())), null));
                }else {
                    locationHints.add(new VertexLocationHint.TaskLocationHint(null, null));
                }
            }

            List<Event> events = Lists.newArrayListWithCapacity(numTasks + 1);

            RootInputConfigureVertexTasksEvent configureVertexEvent = new RootInputConfigureVertexTasksEvent(
                    numTasks, locationHints);
            events.add(configureVertexEvent);
            int count = 0;
            for (InputSplit split : splits) {
                RootInputDataInformationEvent diEvent = new RootInputDataInformationEvent(count++, split);
                events.add(diEvent);
            }
            return events;
        }

}
