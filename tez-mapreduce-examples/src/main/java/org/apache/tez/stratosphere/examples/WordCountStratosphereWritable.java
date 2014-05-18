/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.stratosphere.examples;

import com.google.common.base.Preconditions;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.base.IntSerializer;
import eu.stratosphere.api.common.typeutils.base.StringSerializer;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.runtime.TupleSerializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.*;
import org.apache.tez.dag.api.*;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.committer.MROutputCommitter;
import org.apache.tez.mapreduce.common.MRInputAMSplitGenerator;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.input.ShuffledMergedInput;
import org.apache.tez.runtime.library.output.OnFileSortedOutput;
import org.apache.tez.stratosphere.TupleWritable;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;


public class WordCountStratosphereWritable extends Configured implements Tool {

    public static class TokenProcessor extends SimpleMRProcessor {
        //IntWritable one = new IntWritable(1);
        //Text word = new Text();

        private static final Log LOG = LogFactory.getLog(TokenProcessor.class);

        @Override
        public void run() throws Exception {
            //LOG.fatal("In run Method");
            Preconditions.checkArgument(getInputs().size() == 1);
            Preconditions.checkArgument(getOutputs().size() == 1);
            MRInput input = (MRInput) getInputs().values().iterator().next();
            KeyValueReader kvReader = input.getReader();
            OnFileSortedOutput output = (OnFileSortedOutput) getOutputs().values().iterator().next();
            KeyValueWriter kvWriter = (KeyValueWriter) output.getWriter();
            while (kvReader.next()) {
                StringTokenizer itr = new StringTokenizer(kvReader.getCurrentValue().toString());
                while (itr.hasMoreTokens()) {
                    Tuple2<String, Integer> t = new Tuple2<String, Integer>(itr.nextToken(), new Integer(1));
                    kvWriter.write(new TupleWritable(t), new TupleWritable(t));
                }
            }
        }

    }

    public static class SumProcessor extends SimpleMRProcessor {
        @Override
        public void run() throws Exception {
            Preconditions.checkArgument(getInputs().size() == 1);
            MROutput out = (MROutput) getOutputs().values().iterator().next();
            KeyValueWriter kvWriter = out.getWriter();
            KeyValuesReader kvReader = (KeyValuesReader) getInputs().values().iterator().next()
                    .getReader();
            while (kvReader.next()) {
                TupleWritable word = (TupleWritable) kvReader.getCurrentKey();
                int sum = 0;
                for (Object value : kvReader.getCurrentValues()) {
                    sum += ((Tuple2<String, Integer>) ((TupleWritable)value).getTuple()).f1;
                }
                kvWriter.write(new Text((String)word.getTuple().f0 + "STRATOSPHERE!!!"), new IntWritable(sum));
            }
        }
    }

    private static final Log LOG = LogFactory.getLog(WordCountStratosphereWritable.class);

  private DAG createDAG(FileSystem fs, TezConfiguration tezConf,
      Map<String, LocalResource> localResources, Path stagingDir,
      String inputPath, String outputPath) throws IOException {

    Configuration inputConf = new Configuration(tezConf);
    inputConf.set(FileInputFormat.INPUT_DIR, inputPath);
    InputDescriptor id = new InputDescriptor(MRInput.class.getName())
        .setUserPayload(MRInput.createUserPayload(inputConf,
            TextInputFormat.class.getName(), true, true));

    Configuration outputConf = new Configuration(tezConf);
    outputConf.set(FileOutputFormat.OUTDIR, outputPath);
    OutputDescriptor od = new OutputDescriptor(MROutput.class.getName())
      .setUserPayload(MROutput.createUserPayload(
          outputConf, TextOutputFormat.class.getName(), true));
    
    byte[] intermediateDataPayload = 
        MRHelpers.createMRIntermediateDataPayload(tezConf, TupleWritable.class.getName(),
                TupleWritable.class.getName(), false, null, null);
    
    Vertex tokenizerVertex = new Vertex("tokenizer", new ProcessorDescriptor(
        TokenProcessor.class.getName()), -1, MRHelpers.getMapResource(tezConf));
    tokenizerVertex.setJavaOpts(MRHelpers.getMapJavaOpts(tezConf));
    tokenizerVertex.addInput("MRInput", id, MRInputAMSplitGenerator.class);

    Vertex summerVertex = new Vertex("summer",
        new ProcessorDescriptor(
            SumProcessor.class.getName()), 1, MRHelpers.getReduceResource(tezConf));
    summerVertex.setJavaOpts(
        MRHelpers.getReduceJavaOpts(tezConf));
    summerVertex.addOutput("MROutput", od, MROutputCommitter.class);
    
    DAG dag = new DAG("WordCount");
      dag.addVertex(tokenizerVertex)
              .addVertex(summerVertex)
              .addEdge(
                      new Edge(tokenizerVertex, summerVertex, new EdgeProperty(
                              DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
                              SchedulingType.SEQUENTIAL,
                              new OutputDescriptor(OnFileSortedOutput.class.getName())
                                      .setUserPayload(intermediateDataPayload),
                              new InputDescriptor(ShuffledMergedInput.class.getName())
                                      .setUserPayload(intermediateDataPayload))));

      return dag;
  }

  private static void printUsage() {
    System.err.println("Usage: " + " wordcount <in1> <out1>");
    ToolRunner.printGenericCommandUsage(System.err);
  }

  private Credentials credentials = new Credentials();
  
  public boolean run(String inputPath, String outputPath, Configuration conf) throws Exception {
    System.out.println("Running StratosphereWritableTest");
    // conf and UGI
    TezConfiguration tezConf;
    if (conf != null) {
      tezConf = new TezConfiguration(conf);
    } else {
      tezConf = new TezConfiguration();
    }

    UserGroupInformation.setConfiguration(tezConf);
    String user = UserGroupInformation.getCurrentUser().getShortUserName();

    TezClient tezClient = new TezClient(tezConf);
    ApplicationId appId = tezClient.createApplication();
    
    // staging dir
    FileSystem fs = FileSystem.get(tezConf);
    String stagingDirStr = Path.SEPARATOR + "user" + Path.SEPARATOR
        + user + Path.SEPARATOR+ ".staging" + Path.SEPARATOR
        + Path.SEPARATOR + appId.toString();    
    Path stagingDir = new Path(stagingDirStr);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirStr);
    stagingDir = fs.makeQualified(stagingDir);
    
    // security
    TokenCache.obtainTokensForNamenodes(credentials, new Path[] {stagingDir}, tezConf);
    TezClientUtils.ensureStagingDirExists(tezConf, stagingDir);

    tezConf.set(TezConfiguration.TEZ_AM_JAVA_OPTS,
        MRHelpers.getMRAMJavaOpts(tezConf));

    // No need to add jar containing this class as assumed to be part of
    // the tez jars.

    // TEZ-674 Obtain tokens based on the Input / Output paths. For now assuming staging dir
    // is the same filesystem as the one used for Input/Output.
    
    TezSession tezSession = null;
    AMConfiguration amConfig = new AMConfiguration(null,
        null, tezConf, credentials);
    
    TezSessionConfiguration sessionConfig =
        new TezSessionConfiguration(amConfig, tezConf);
    tezSession = new TezSession("WordCountSession", appId,
        sessionConfig);
    tezSession.start();

    DAGClient dagClient = null;

    try {
        if (fs.exists(new Path(outputPath))) {
          throw new FileAlreadyExistsException("Output directory "
              + outputPath + " already exists");
        }
        
        Map<String, LocalResource> localResources =
          new TreeMap<String, LocalResource>();
        
        DAG dag = createDAG(fs, tezConf, localResources,
            stagingDir, inputPath, outputPath);

        tezSession.waitTillReady();
        dagClient = tezSession.submitDAG(dag);

        // monitoring
        DAGStatus dagStatus = dagClient.waitForCompletionWithAllStatusUpdates(null);
        if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
          System.out.println("DAG diagnostics: " + dagStatus.getDiagnostics());
          return false;
        }
        return true;
    } finally {
      fs.delete(stagingDir, true);
      tezSession.stop();
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 2) {
      printUsage();
      return 2;
    }
    WordCountStratosphereWritable job = new WordCountStratosphereWritable();
    job.run(otherArgs[0], otherArgs[1], conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new WordCountStratosphereWritable(), args);
    System.exit(res);
  }
}
