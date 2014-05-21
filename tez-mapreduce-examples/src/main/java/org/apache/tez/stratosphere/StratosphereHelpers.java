package org.apache.tez.stratosphere;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import eu.stratosphere.api.java.io.TextInputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.*;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.core.io.InputSplit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezUtils;
import org.apache.tez.mapreduce.combine.MRCombiner;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfToTezTranslator;
import org.apache.tez.mapreduce.partition.MRPartitioner;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by filip on 19.05.14.
 */
public class StratosphereHelpers {

    private static final Log LOG = LogFactory.getLog(StratosphereHelpers.class);

    public static String CONF_INPUT_FILE = "stratosphere.input.file.path";
    public static String CONF_OUTPUT_FILE = "stratosphere.output.file.path";

    //  From stratosphere AbstractFileInput
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @InterfaceAudience.Private
    public static FileInputSplit[] generateSplits(
            JobContext jobContext, String inputFormatName, int numTasks)
            throws ClassNotFoundException, IOException,
            InterruptedException {

        final String pathURI = jobContext.getConfiguration().get(StratosphereHelpers.CONF_INPUT_FILE);
        TextInputFormat inputFormat = new TextInputFormat(new Path(pathURI));
        inputFormat.configure(new Configuration());
        return inputFormat.createInputSplits(1);
    }

    /**
     * Sets up parameters which used to be set by the MR JobClient. Includes
     * setting whether to use the new api or the old api. Note: Must be called
     * before generating InputSplits
     *
     * @param conf
     *          configuration for the vertex.
     */
    public static void doJobClientMagic(org.apache.hadoop.conf.Configuration conf) throws IOException {
        // TODO Maybe add functionality to check output specifications - e.g. fail
        // early if the output directory exists.
        InetAddress ip = InetAddress.getLocalHost();
        if (ip != null) {
            String submitHostAddress = ip.getHostAddress();
            String submitHostName = ip.getHostName();
            conf.set(MRJobConfig.JOB_SUBMITHOST, submitHostName);
            conf.set(MRJobConfig.JOB_SUBMITHOSTADDR, submitHostAddress);
        }

        // TODO eventually ACLs
        conf.set(TezJobConfig.TEZ_RUNTIME_PARTITIONER_CLASS, MRPartitioner.class.getName());

        // TODO Set Combiner
        //conf.set(TezJobConfig.TEZ_RUNTIME_COMBINER_CLASS, MRCombiner.class.getName());

        setWorkingDirectory(conf);
    }

    private static void setWorkingDirectory(org.apache.hadoop.conf.Configuration conf) {
        String name = conf.get(JobContext.WORKING_DIR);
        if (name == null) {
            try {
                org.apache.hadoop.fs.Path dir = org.apache.hadoop.fs.FileSystem.get(conf).getWorkingDirectory();
                conf.set(JobContext.WORKING_DIR, dir.toString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static byte[] createMRInputPayload(org.apache.hadoop.conf.Configuration conf) throws IOException {
        Preconditions.checkArgument(conf != null, "Configuration must be specified");
        MRRuntimeProtos.MRInputUserPayloadProto.Builder userPayloadBuilder = MRRuntimeProtos.MRInputUserPayloadProto
                .newBuilder();
        userPayloadBuilder.setConfigurationBytes(createByteStringFromConf(conf));
        return userPayloadBuilder.build().toByteArray();
    }

    @InterfaceAudience.LimitedPrivate("Hive, Pig")
    public static ByteString createByteStringFromConf(org.apache.hadoop.conf.Configuration conf)
            throws IOException {
        return TezUtils.createByteStringFromConf(conf);
    }

    @InterfaceAudience.LimitedPrivate("Hive, Pig")
    public static org.apache.hadoop.conf.Configuration createConfFromByteString(ByteString bs)
            throws IOException {
        return TezUtils.createConfFromByteString(bs);
    }

    public static MRRuntimeProtos.MRInputUserPayloadProto parseMRInputPayload(byte[] bytes)
            throws IOException {
        return MRRuntimeProtos.MRInputUserPayloadProto.parseFrom(bytes);
    }

    /**
     * Create the user payload to be set on intermediate edge Input/Output classes
     * that use MapReduce Key-Value data types. If the input and output have
     * different configurations then this method may be called separately for both
     * to get different payloads. If the input and output have no special
     * configuration then this method may be called once to get the common payload
     * for both input and output.
     *
     * @param conf
     *          Configuration for the class
     * @param className
     *          Class name of the elements
     * Could @param keyComparator add here
     *          Optional key comparator class name
     * @return
     * @throws IOException
     */
    public static byte[] createMRIntermediateDataPayload(org.apache.hadoop.conf.Configuration conf,
                                                         String className) throws IOException {
        Preconditions.checkNotNull(conf);
        Preconditions.checkNotNull(className);
        org.apache.hadoop.conf.Configuration intermediateDataConf = new JobConf(conf);
        intermediateDataConf.set(StratosphereJobConfig.OUTPUT_CLASS, className);

        MultiStageMRConfToTezTranslator.translateVertexConfToTez(
                intermediateDataConf, intermediateDataConf);
        StratosphereHelpers.doJobClientMagic(intermediateDataConf);

        return TezUtils.createUserPayloadFromConf(intermediateDataConf);
    }

    /**
     * Extract the map task's container resource requirements from the
     * provided configuration.
     *
     * Uses values from the provided configuration.
     *
     * @param conf Configuration with MR specific settings used to extract
     * information from
     *
     * @return Resource object used to define requirements for containers
     * running Map tasks
     */
    public static Resource getResource(org.apache.hadoop.conf.Configuration conf) {
        return Resource.newInstance(conf.getInt(
                        StratosphereJobConfig.PROCESSOR_MEMORY_MB, StratosphereJobConfig.DEFAULT_PROCESSOR_MEMORY_MB),
                conf.getInt(StratosphereJobConfig.PROCESSOR_CPU_VCORES,
                        StratosphereJobConfig.DEFAULT_PROCESSOR_CPU_VCORES));
    }

    /**
     * Extract Java Opts for the AM based on MR-based configuration
     * @param conf Configuration from which to extract information
     * @return Java opts for the AM
     */
    public static String getMRAMJavaOpts(org.apache.hadoop.conf.Configuration conf) {
        // Admin opts
        String mrAppMasterAdminOptions = conf.get(
                StratosphereJobConfig.AM_ADMIN_COMMAND_OPTS,
                StratosphereJobConfig.DEFAULT_AM_ADMIN_COMMAND_OPTS);
        // Add AM user command opts
        String mrAppMasterUserOptions = conf.get(StratosphereJobConfig.AM_COMMAND_OPTS,
                StratosphereJobConfig.DEFAULT_AM_COMMAND_OPTS);

        return mrAppMasterAdminOptions.trim()
                + " " + mrAppMasterUserOptions.trim();
    }

}
