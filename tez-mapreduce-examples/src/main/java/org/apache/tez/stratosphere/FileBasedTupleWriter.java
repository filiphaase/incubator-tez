package org.apache.tez.stratosphere;

import com.google.common.base.Preconditions;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.base.StringSerializer;
import eu.stratosphere.api.common.typeutils.base.IntSerializer;

import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.runtime.TupleSerializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.TezOutputContext;
import org.apache.tez.runtime.api.Writer;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutput;

import java.io.*;
import java.util.Arrays;

/**
 * Created by filip on 15.05.14.
 */
public class FileBasedTupleWriter implements Writer{

    private static final Log LOG = LogFactory.getLog(FileBasedTupleWriter.class);

    public static final int INDEX_RECORD_LENGTH = 24;

    private final Configuration conf;
    private int numRecords = 0;

    /*@SuppressWarnings("rawtypes")
    private final Class keyClass;
    @SuppressWarnings("rawtypes")
    private final Class valClass;
    private final CompressionCodec codec;*/
    private final FileSystem rfs;
    //private final IFile.Writer writer;

    private final Path outputPath;
    private Path indexPath;

    private final TezTaskOutput outputFileManager;
    private boolean closed = false;

    // Number of output key-value pairs
    private final TezCounter outputRecordsCounter;
    // Number of bytes of actual output - uncompressed.
    private final TezCounter outputBytesCounter;
    // Size of the data with additional meta-data
    private final TezCounter outputBytesCounterWithOverhead;
    // Actual physical size of the data on disk.
    private final TezCounter outputMaterializedBytesCounter;

    private final IFile.Writer writer;
    private final TypeSerializer serializer;


    public FileBasedTupleWriter(TezOutputContext outputContext, Configuration conf) throws IOException {
        this.conf = conf;

        this.outputRecordsCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_RECORDS);
        this.outputBytesCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES);
        this.outputBytesCounterWithOverhead = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_WITH_OVERHEAD);
        this.outputMaterializedBytesCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_PHYSICAL);

        // Get Filesystem and output Path
        this.rfs = ((LocalFileSystem) FileSystem.getLocal(this.conf)).getRaw();
        this.outputFileManager = TezRuntimeUtils.instantiateTaskOutputManager(conf,
                outputContext);
        this.outputPath = outputFileManager.getOutputFileForWrite();

        // Set key, and value classes (param 4,5) to null let the Writer not initialize a serializer
        this.writer = new IFile.Writer(conf, rfs, outputPath, null, null,
                null, null, outputBytesCounter);

        this.serializer = new TupleSerializer(Tuple2.class, new TypeSerializer[] {
                    new StringSerializer(),
                    new IntSerializer()
        });


    }

    public void write(Tuple2 input) throws IOException{
        OutputViewHelper outputView = new OutputViewHelper();
        // Setup output view and serialize
        this.serializer.serialize(input, outputView);

        // Read back reserialized bytes and write into input buffer
        byte[] dataBuffer = outputView.getWrittenData();
        DataInputBuffer keyBuffer = new DataInputBuffer();
        DataInputBuffer valueBuffer = new DataInputBuffer();
        keyBuffer.reset(dataBuffer, dataBuffer.length);
        valueBuffer.reset(dataBuffer, dataBuffer.length);

        // write into TEZ Writer
        this.writer.append(keyBuffer, valueBuffer);
        numRecords++;
    }

    // !!!!!!!!!!!!!!!        JUST COPY PASTER SO FAR FROM FileBasedKeyValueWriter
    /**
     * @return true if any output was generated. false otherwise
     * @throws IOException
     */
    public boolean close() throws IOException {
        this.closed = true;
        this.writer.close();
        long rawLen = writer.getRawLength();
        long compLen = writer.getCompressedLength();
        outputBytesCounterWithOverhead.increment(rawLen);
        outputMaterializedBytesCounter.increment(compLen);
        TezIndexRecord rec = new TezIndexRecord(0, rawLen, compLen);
        TezSpillRecord sr = new TezSpillRecord(1);
        sr.putIndex(rec, 0);

        LOG.info("returning numRecords: " + numRecords);
        this.indexPath = outputFileManager
                .getOutputIndexFileForWrite(INDEX_RECORD_LENGTH);
        LOG.info("Writing index file: " + indexPath);
        sr.writeToFile(indexPath, conf);
        return numRecords > 0;
    }

    public long getRawLength() {
        Preconditions.checkState(closed, "Only available after the Writer has been closed");
        return this.writer.getRawLength();
    }

    public long getCompressedLength() {
        Preconditions.checkState(closed, "Only available after the Writer has been closed");
        return this.writer.getCompressedLength();
    }

    public byte[] getData() throws IOException {
        Preconditions.checkState(closed,
                "Only available after the Writer has been closed");
        FSDataInputStream inStream = null;
        byte[] buf = null;
        try {
            inStream = rfs.open(outputPath);
            buf = new byte[(int) getCompressedLength()];
            IOUtils.readFully(inStream, buf, 0, (int) getCompressedLength());
        } finally {
            if (inStream != null) {
                inStream.close();
            }
        }
        return buf;
    }

}
