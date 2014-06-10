package org.apache.tez.stratosphere;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.util.MutableObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.api.TezOutputContext;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.impl.MergeManager;
import org.apache.tez.runtime.library.common.sort.impl.IFileInputStream;
import org.apache.tez.runtime.library.common.sort.impl.IFileOutputStream;

import java.io.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by filip on 18.05.14.
 */
public class StratosphereIFile {

        private static final Log LOG = LogFactory.getLog(StratosphereIFile.class);
        public static final int EOF_MARKER = -1; // End of File Marker

        /**
         * <code>IFile.Writer</code> to write out intermediate map-outputs.
         */
        @InterfaceAudience.Private
        @InterfaceStability.Unstable
        @SuppressWarnings({"unchecked", "rawtypes"})
        public static class Writer<T> {
            FSDataOutputStream out;
            boolean ownOutputStream = false;
            long start = 0;
            FSDataOutputStream rawOut;
            AtomicBoolean closed = new AtomicBoolean(false);

            long decompressedBytesWritten = 0;
            long compressedBytesWritten = 0;

            // Count records written to disk
            private long numRecordsWritten = 0;
            private final TezCounter writtenRecordsCounter;
            private final TezCounter serializedBytes;

            private final FileSystem fs;
            private final Path filePath;
            IFileOutputStream checksumOut;

            Class clazz;
            TypeSerializer<T> serializer;

            DataOutputBuffer buffer = new DataOutputBuffer();

            public Writer(Configuration conf, FileSystem fs, Path file,
                          Class clazz, TezCounter writesCounter, TezCounter serializedBytesCounter,
                          TypeSerializer<T> serializer)
                    throws IOException {
                FSDataOutputStream out = fs.create(file);

                this.writtenRecordsCounter = writesCounter;
                this.serializedBytes = serializedBytesCounter;
                this.checksumOut = new IFileOutputStream(out);
                this.rawOut = out;
                this.start = this.rawOut.getPos();
                this.out = new FSDataOutputStream(checksumOut,null);

                this.clazz = clazz;
                this.serializer = serializer;
                this.filePath = file;
                this.fs = fs;
            }

            public void close() throws IOException {
                if (closed.getAndSet(true)) {
                    throw new IOException("Writer was already closed earlier");
                }

                // Write EOF_MARKER for key/value length
                WritableUtils.writeVInt(out, EOF_MARKER);
                WritableUtils.writeVInt(out, EOF_MARKER);
                decompressedBytesWritten += 2 * WritableUtils.getVIntSize(EOF_MARKER);

                //Flush the stream
                out.flush();

                // Close the underlying stream iff we own it...
                if (ownOutputStream) {
                    out.close();
                }
                else {
                    // Write the checksum
                    checksumOut.finish();
                }

                compressedBytesWritten = rawOut.getPos() - start;
                out = null;
                if(writtenRecordsCounter != null) {
                    writtenRecordsCounter.increment(numRecordsWritten);
                }
            }

            public void append(T element) throws IOException {
                if (element.getClass() != clazz)
                    throw new IOException("wrong key class: "+ element.getClass()
                            +" is not "+ clazz);

                OutputViewHelper outputView = new OutputViewHelper(buffer);
                // Append the element
                serializer.serialize(element, outputView);
                int elementLength = buffer.getLength();
                if (elementLength < 0) {
                    throw new IOException("Negative element-length not allowed: " + elementLength +
                            " for " + element);
                }

                // Write the record out
                WritableUtils.writeVInt(out, elementLength);
                out.write(buffer.getData(),- 0, buffer.getLength());
                // Update bytes written
                decompressedBytesWritten += elementLength +
                        WritableUtils.getVIntSize(elementLength);
                if (serializedBytes != null) {
                    serializedBytes.increment(elementLength);
                }
                // Reset
                buffer.reset();
                ++numRecordsWritten;
            }

            public long getRawLength() {
                return decompressedBytesWritten;
            }

            public long getCompressedLength() {
                return compressedBytesWritten;
            }

            public MutableObjectIterator<T> getMutableObjectIterator(Configuration conf) throws IOException{
                FSDataInputStream inputStream = fs.open(filePath);
                int bufferSize =  conf.getInt("io.file.buffer.size", TezJobConfig.TEZ_RUNTIME_IFILE_BUFFER_SIZE_DEFAULT);

                //LOG.info("gettingMutableObjectIterator - decompressedBytesWritten: " + decompressedBytesWritten);
                //LOG.info("gettingMutableObjectIterator - bufferSize: " + bufferSize);

                return new IFileIterator<T>(
                        new Reader<T>(inputStream, compressedBytesWritten, null, null, false, 0,
                                bufferSize, serializer)
                );
            }

            public static class IFileIterator<T> implements MutableObjectIterator<T>{
                private Reader<T> r;
                public IFileIterator(Reader<T> reader){
                    r = reader;
                }

                @Override
                public T next(T reuse) throws IOException {
                    return r.readElement();
                }
            }
        }


        /**
         * <code>IFile.Reader</code> to read intermediate map-outputs.
         */
        @InterfaceAudience.Private
        @InterfaceStability.Unstable
        public static class Reader<T> {

            private static final int DEFAULT_BUFFER_SIZE = 128*1024;

            // Count records read from disk
            private long numRecordsRead = 0;
            private final TezCounter readRecordsCounter;
            private final TezCounter bytesReadCounter;

            final InputStream in;        // Possibly decompressed stream that we read
            public long bytesRead = 0;
            protected final long fileLength;
            protected boolean eof = false;
            final IFileInputStream checksumIn;

            protected byte[] buffer = null;
            protected int bufferSize = DEFAULT_BUFFER_SIZE;
            protected DataInputStream dataIn;

            protected int recNo = 1;
            protected int currentElementLength;
            byte elementBytes[] = new byte[0];

            long startPos;
            TypeSerializer<T> serializer;
            protected DataInputBuffer elementBuffer = new DataInputBuffer();

            /**
             * Construct an IFile Reader.
             *
             * @param in   The input stream
             * @param length Length of the data in the stream, including the checksum
             *               bytes.
             * @param readsCounter Counter for records read from disk
             * @throws IOException
             */
            public Reader(InputStream in, long length,
                          TezCounter readsCounter, TezCounter bytesReadCounter,
                          boolean readAhead, int readAheadLength,
                          int bufferSize, TypeSerializer serializer) throws IOException {
                readRecordsCounter = readsCounter;
                this.bytesReadCounter = bytesReadCounter;
                checksumIn = new IFileInputStream(in,length, readAhead, readAheadLength);
                this.in = checksumIn;
                this.dataIn = new DataInputStream(this.in);
                this.fileLength = length;
                this.serializer = serializer;
                startPos = checksumIn.getPosition();

                LOG.info("Setting up Reader");

                if (bufferSize != -1) {
                    this.bufferSize = bufferSize;
                }
            }

            public long getLength() {
                return fileLength - checksumIn.getSize();
            }

            /**
             * Read upto len bytes into buf starting at offset off.
             *
             * @param buf buffer
             * @param off offset
             * @param len length of buffer
             * @return the no. of bytes read
             * @throws IOException
             */
            private int readData(byte[] buf, int off, int len) throws IOException {
                int bytesRead = 0;
                while (bytesRead < len) {
                    int n = IOUtils.wrappedReadForCompressedData(in, buf, off + bytesRead,
                            len - bytesRead);
                    if (n < 0) {
                        return bytesRead;
                    }
                    bytesRead += n;
                }
                return len;
            }

            protected boolean positionToNextRecord(DataInput dIn) throws IOException {
                // Sanity check
                if (eof) {
                    throw new EOFException("Completed reading " + bytesRead);
                }

                currentElementLength = WritableUtils.readVInt(dIn);
                bytesRead += WritableUtils.getVIntSize(currentElementLength);

                // Check for EOF
                if (currentElementLength == EOF_MARKER ) {
                    eof = true;
                    return false;
                }

                // Sanity check
                if (currentElementLength < 0) {
                    throw new IOException("Rec# " + recNo + ": Negative length: " +
                            currentElementLength);
                }

                return true;
            }

            public T readElement() throws IOException {
                if (!positionToNextRecord(dataIn)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("currentElementLength=" + currentElementLength +
                                ", bytesRead=" + bytesRead +
                                ", length=" + fileLength);
                    }
                    return null;
                }
                if (elementBytes.length < currentElementLength) {
                    elementBytes = new byte[currentElementLength << 1];
                }
                int i = readData(elementBytes, 0, currentElementLength);
                if (i != currentElementLength) {
                    throw new IOException ("Asked for " + currentElementLength + " Got: " + i);
                }
                elementBuffer.reset(elementBytes, currentElementLength);
                bytesRead += currentElementLength;

                T reuse = serializer.createInstance();
                reuse = serializer.deserialize(reuse, new InputViewHelper(elementBuffer));
                return reuse;
            }

            public void close() throws IOException {
                // Close the underlying stream
                in.close();

                // Release the buffer
                dataIn = null;
                buffer = null;
                if(readRecordsCounter != null) {
                    readRecordsCounter.increment(numRecordsRead);
                }

                if (bytesReadCounter != null) {
                    bytesReadCounter.increment(checksumIn.getPosition() - startPos + checksumIn.getSize());
                }
            }

            public void reset(int offset) {
                return;
            }
        }

        public static class InMemoryReader<T> extends Reader<T>{

            private final InputAttemptIdentifier taskAttemptId;
            private final MergeManager merger;
            DataInputBuffer memDataIn = new DataInputBuffer();
            private int start;
            private int length;

            public InMemoryReader(MergeManager merger, InputAttemptIdentifier taskAttemptId,
                                  byte[] data, int start, int length, TypeSerializer<T> serializer)
                    throws IOException {
                super(null, length - start, null, null, false, 0, -1, serializer);
                this.merger = merger;
                this.taskAttemptId = taskAttemptId;

                buffer = data;
                bufferSize = (int)fileLength;
                memDataIn.reset(buffer, start, length);
                this.start = start;
                this.length = length;
            }

            @Override
            public void reset(int offset) {
                memDataIn.reset(buffer, start + offset, length);
                bytesRead = offset;
                eof = false;
            }

            @Override
            public long getLength() {
                return fileLength;
            }

            private void dumpOnError() {
                File dumpFile = new File("../output/" + taskAttemptId + ".dump");
                System.err.println("Dumping corrupt output of " + taskAttemptId +
                        " to " + dumpFile.getAbsolutePath());
                try {
                    FileOutputStream fos = new FileOutputStream(dumpFile);
                    fos.write(buffer, 0, bufferSize);
                    fos.close();
                } catch (IOException ioe) {
                    System.err.println("Failed to dump map-output of " + taskAttemptId);
                }
            }

            @Override
            public T readElement() throws IOException {
                try {
                    if (!positionToNextRecord(memDataIn)) {
                        return null;
                    }
                    // Setup the key
                    int pos = memDataIn.getPosition();
                    byte[] data = memDataIn.getData();
                    elementBuffer.reset(data, pos, currentElementLength);

                    // Position for the next value
                    long skipped = memDataIn.skip(currentElementLength);
                    if (skipped != currentElementLength) {
                        throw new IOException("Rec# " + recNo +
                                ": Failed to skip past key of length: " +
                                currentElementLength);
                    }

                    // Record the byte
                    bytesRead += currentElementLength;

                    T reuse = serializer.createInstance();
                    reuse = serializer.deserialize(reuse, new InputViewHelper(elementBuffer));
                    return reuse;
                } catch (IOException ioe) {
                    dumpOnError();
                    throw ioe;
                }
            }

            public void close() {
                // Release
                dataIn = null;
                buffer = null;
                // Inform the MergeManager
                if (merger != null) {
                    // TODO FH had to outcomment this because of visibility, that's propably bad
                    //merger.unreserve(bufferSize);
                }
            }
        }
}
