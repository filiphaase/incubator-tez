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
import eu.stratosphere.api.java.io.TextOutputFormat;
import eu.stratosphere.api.java.tuple.Tuple2;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.*;
import org.apache.tez.dag.api.*;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.runtime.library.processor.SimpleProcessor;
import org.apache.tez.stratosphere.*;

import java.io.IOException;
import java.util.StringTokenizer;


public class WordCount extends Configured implements Tool {
  public static class TokenProcessor extends SimpleProcessor {

    private static final Log LOG = LogFactory.getLog(TokenProcessor.class);

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(getInputs().size() == 1);
      Preconditions.checkArgument(getOutputs().size() == 1);

      StratosphereInput<String> input = (StratosphereInput<String>) getInputs().values().iterator().next();
      StratosphereReader<String> reader = input.getReader();

      OnFileOrderedStratosphereOutput output = (OnFileOrderedStratosphereOutput) getOutputs().values().iterator().next();
      StratosphereWriter<Tuple2<String, Integer>> tupleWriter = output.getWriter();

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

  public static class SumProcessor extends SimpleProcessor {

    private static final Log LOG = LogFactory.getLog(SumProcessor.class);

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(getInputs().size() == 1);

      StratosphereOutput out = (StratosphereOutput) getOutputs().values().iterator().next();
      StratosphereWriter<Tuple2<String, Integer>> writer = out.getWriter();

      ShuffledUnorderedStratosphereInput tupleInput = (ShuffledUnorderedStratosphereInput) getInputs().values().iterator().next();
      StratosphereReader<Tuple2<String,Integer>> tupleReader = tupleInput.getReader();

      if(!tupleReader.hasNext())
          return;

      int sum = 1;
      Tuple2<String, Integer> curr = tupleReader.getNext();
      String currentKey = curr.f0;

      while (tupleReader.hasNext()) {
        curr = tupleReader.getNext();
        if(curr.f0.equals(currentKey)){
            sum += 1;
        }else{
            writer.write(new Tuple2<String, Integer>(currentKey, sum));
            currentKey = curr.f0;
            sum = 1;
        }
      }
      writer.write(new Tuple2<String, Integer>(currentKey, sum));
    }
  }

  private DAG createDAG(TezConfiguration tezConf,
      String inputPath, String outputPath) throws IOException {

    Configuration inputConf = new Configuration(tezConf);
    inputConf.set(StratosphereHelpers.CONF_INPUT_FILE, inputPath);
    InputDescriptor id = new InputDescriptor(StratosphereInput.class.getName())
        .setUserPayload(StratosphereInput.createUserPayload(inputConf,
                TextInputFormat.class.getName()));

    Configuration outputConf = new Configuration(tezConf);
    outputConf.set(StratosphereHelpers.CONF_OUTPUT_FILE, outputPath);
    OutputDescriptor od = new OutputDescriptor(StratosphereOutput.class.getName())
      .setUserPayload(StratosphereOutput.createUserPayload(
          outputConf, TextOutputFormat.class.getName()));
    
    byte[] intermediateDataPayload = 
        StratosphereHelpers.createMRIntermediateDataPayload(tezConf, Tuple2.class.getName());
    
    Vertex tokenizerVertex = new Vertex("tokenizer", new ProcessorDescriptor(
        TokenProcessor.class.getName()), -1, StratosphereHelpers.getResource(tezConf));
    tokenizerVertex.addInput("StratosphereInput", id, StratosphereInputAMSplitGenerator.class);

    Vertex summerVertex = new Vertex("summer",
        new ProcessorDescriptor(
            SumProcessor.class.getName()), 1, StratosphereHelpers.getResource(tezConf));
    summerVertex.addOutput("StratosphereOutput", od, null);
    
    DAG dag = new DAG("Stratosphere Wordcount");
    dag.addVertex(tokenizerVertex)
        .addVertex(summerVertex)
        .addEdge(
            new Edge(tokenizerVertex, summerVertex, new EdgeProperty(
                DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
                SchedulingType.SEQUENTIAL,
                new OutputDescriptor(OnFileOrderedStratosphereOutput.class.getName())
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
    System.out.println("Running StratosphereWordCount");
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
        StratosphereHelpers.getMRAMJavaOpts(tezConf));

    // No need to add jar containing this class as assumed to be part of
    // the tez jars.

    // TEZ-674 Obtain tokens based on the Input / Output paths. For now assuming staging dir
    // is the same filesystem as the one used for Input/Output.
    
    TezSession tezSession = null;
    AMConfiguration amConfig = new AMConfiguration(null,
        null, tezConf, credentials);
    
    TezSessionConfiguration sessionConfig =
        new TezSessionConfiguration(amConfig, tezConf);
    tezSession = new TezSession("StratosphereWordCount", appId,
        sessionConfig);
    tezSession.start();

    DAGClient dagClient = null;

    try {
        if (fs.exists(new Path(outputPath))) {
          throw new FileAlreadyExistsException("Output directory "
              + outputPath + " already exists");
        }
        
        DAG dag = createDAG(tezConf, inputPath, outputPath);

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
    WordCount job = new WordCount();
    job.run(otherArgs[0], otherArgs[1], conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new WordCount(), args);
    System.exit(res);
  }
}
