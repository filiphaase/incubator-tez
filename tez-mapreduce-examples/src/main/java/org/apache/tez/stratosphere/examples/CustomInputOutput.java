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
import eu.stratosphere.api.java.io.TextInputFormat;
import eu.stratosphere.api.java.tuple.Tuple2;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
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
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.stratosphere.*;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;


public class CustomInputOutput extends Configured implements Tool {
  public static class TokenProcessor extends SimpleMRProcessor {

      private static final Log LOG = LogFactory.getLog(TokenProcessor.class);

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(getInputs().size() == 1);
      Preconditions.checkArgument(getOutputs().size() == 1);

      StratosphereInput<String> input = (StratosphereInput<String>) getInputs().values().iterator().next();
      StratosphereReader<String> reader = input.getReader();

      OnFileUnorderedStratosphereOutput output = (OnFileUnorderedStratosphereOutput) getOutputs().values().iterator().next();
      FileBasedTupleWriter tupleWriter = output.getWriter();

      while (reader.hasNext()) {
        String s = reader.getNext();
        if(s == null)
            continue;
        StringTokenizer itr = new StringTokenizer(s);
        while (itr.hasMoreTokens()) {
          tupleWriter.write(new Tuple2<String, Integer>(itr.nextToken(), 1));
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
      ShuffledUnorderedStratosphereInput tupleInput = (ShuffledUnorderedStratosphereInput) getInputs().values().iterator().next();
      ShuffledUnorderedTupleReader tupleReader = tupleInput.getReader();
      while (tupleReader.next()) {
        Tuple2<String, Integer> curr = (Tuple2<String, Integer>)tupleReader.getCurrentTuple();
        kvWriter.write(new Text("Stratosphere " + curr.f0), new IntWritable(curr.f1));
      }
    }
  }

  private DAG createDAG(FileSystem fs, TezConfiguration tezConf,
      Map<String, LocalResource> localResources, Path stagingDir,
      String inputPath, String outputPath) throws IOException {

    Configuration inputConf = new Configuration(tezConf);
    inputConf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR, inputPath);
    InputDescriptor id = new InputDescriptor(StratosphereInput.class.getName())
        .setUserPayload(StratosphereInput.createUserPayload(inputConf,
                TextInputFormat.class.getName()));

    Configuration outputConf = new Configuration(tezConf);
    outputConf.set(FileOutputFormat.OUTDIR, outputPath);
    OutputDescriptor od = new OutputDescriptor(MROutput.class.getName())
      .setUserPayload(MROutput.createUserPayload(
          outputConf, TextOutputFormat.class.getName(), true));
    
    byte[] intermediateDataPayload = 
        MRHelpers.createMRIntermediateDataPayload(tezConf, Tuple2.class.getName(),
            Tuple2.class.getName(), true, null, null);
    
    Vertex tokenizerVertex = new Vertex("tokenizer", new ProcessorDescriptor(
        TokenProcessor.class.getName()), -1, MRHelpers.getMapResource(tezConf));
    tokenizerVertex.setJavaOpts(MRHelpers.getMapJavaOpts(tezConf));
    tokenizerVertex.addInput("StratosphereInput", id, StratosphereInputAMSplitGenerator.class);

    Vertex summerVertex = new Vertex("summer",
        new ProcessorDescriptor(
            SumProcessor.class.getName()), 1, MRHelpers.getReduceResource(tezConf));
    summerVertex.setJavaOpts(
        MRHelpers.getReduceJavaOpts(tezConf));
    summerVertex.addOutput("MROutput", od, MROutputCommitter.class);
    
    DAG dag = new DAG("StratosphereCustomInputOutput");
    dag.addVertex(tokenizerVertex)
        .addVertex(summerVertex)
        .addEdge(
            new Edge(tokenizerVertex, summerVertex, new EdgeProperty(
                DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
                SchedulingType.SEQUENTIAL,
                new OutputDescriptor(OnFileUnorderedStratosphereOutput.class.getName())
                        .setUserPayload(intermediateDataPayload),
                new InputDescriptor(ShuffledUnorderedStratosphereInput.class.getName())
                        .setUserPayload(intermediateDataPayload))));
    return dag;
  }

  private static void printUsage() {
    System.err.println("Usage: " + " wordcount <in1> <out1>");
    ToolRunner.printGenericCommandUsage(System.err);
  }

  private Credentials credentials = new Credentials();
  
  public boolean run(String inputPath, String outputPath, Configuration conf) throws Exception {
    System.out.println("Running StratosphereCustomInputOutput");
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
    CustomInputOutput job = new CustomInputOutput();
    job.run(otherArgs[0], otherArgs[1], conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CustomInputOutput(), args);
    System.exit(res);
  }
}
