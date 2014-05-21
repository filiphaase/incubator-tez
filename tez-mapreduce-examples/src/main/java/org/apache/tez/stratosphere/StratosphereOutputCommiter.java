package org.apache.tez.stratosphere;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.mapreduce.hadoop.MRConfig;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.runtime.api.*;

import java.io.IOException;

/**
 * Created by filip on 21.05.14.
 */
public class StratosphereOutputCommiter extends org.apache.tez.runtime.api.OutputCommitter{

    private static final Log LOG = LogFactory.getLog(StratosphereOutputCommiter.class);


    @Override
    public void initialize(OutputCommitterContext context) throws IOException {
    }

    @Override
    public void setupOutput() throws IOException {
    }

    @Override
    public void commitOutput() throws IOException {
    }

    @Override
    public void abortOutput(VertexStatus.State finalState) throws IOException {
    }

    @Override
    public boolean isTaskRecoverySupported() {
        return false;
    }

    @Override
    public void recoverTask(int taskIndex, int attemptId) throws IOException {

    }

}
