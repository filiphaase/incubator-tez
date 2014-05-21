package org.apache.tez.stratosphere;

/**
 * Created by filip on 21.05.14.
 */
public class StratosphereJobConfig {

    public static String OUTPUT_CLASS = "stratosphere.output.class";

    public static final String PROCESSOR_MEMORY_MB = "stratosphere.processor.memory.mb";
    public static final int DEFAULT_PROCESSOR_MEMORY_MB = 1024;

    public static final String PROCESSOR_CPU_VCORES = "stratosphere.processor.cpu.vcores";
    public static final int DEFAULT_PROCESSOR_CPU_VCORES = 1;

    public static final String STRATOSPHERE_PREFIX = "yarn.app.stratosphere.";

    public static final String STRATOSPHERE_AM_PREFIX = STRATOSPHERE_PREFIX + "am.";

    /** Admin command opts passed to the MR app master.*/
    public static final String AM_ADMIN_COMMAND_OPTS =
            STRATOSPHERE_AM_PREFIX+"admin-command-opts";
    public static final String DEFAULT_AM_ADMIN_COMMAND_OPTS = "";

    /** Command line arguments passed to the MR app master.*/
    public static final String AM_COMMAND_OPTS =
            STRATOSPHERE_AM_PREFIX+"command-opts";
    public static final String DEFAULT_AM_COMMAND_OPTS = "-Xmx1024m";
}
