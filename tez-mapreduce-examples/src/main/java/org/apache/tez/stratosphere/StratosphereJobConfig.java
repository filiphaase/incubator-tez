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
}
