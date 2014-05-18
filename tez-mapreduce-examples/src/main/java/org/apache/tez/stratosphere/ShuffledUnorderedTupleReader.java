package org.apache.tez.stratosphere;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.base.IntSerializer;
import eu.stratosphere.api.common.typeutils.base.StringSerializer;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.runtime.TupleSerializer;
import eu.stratosphere.core.memory.DataInputView;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.shuffle.impl.InMemoryReader;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.shuffle.common.FetchedInput;
import org.apache.tez.runtime.library.shuffle.common.MemoryFetchedInput;
import org.apache.tez.runtime.library.shuffle.common.impl.ShuffleManager;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Created by filip on 15.05.14.
 */
public class ShuffledUnorderedTupleReader<T> implements Reader{

    private static final Log LOG = LogFactory.getLog(ShuffledUnorderedTupleReader.class);

    private final ShuffleManager shuffleManager;
    private final CompressionCodec codec;

    private final Class<T> tupleClass;
    private final TypeSerializer<T> tupleSerializer;
    private final DataInputBuffer keyIn;
    private final DataInputBuffer valIn;

    private final boolean ifileReadAhead;
    private final int ifileReadAheadLength;
    private final int ifileBufferSize;

    private final TezCounter inputRecordCounter;

    private T tuple;

    private FetchedInput currentFetchedInput;
    private IFile.Reader currentReader;

    // TODO Remove this once per I/O counters are separated properly. Relying on
    // the counter at the moment will generate aggregate numbers.
    private int numRecordsRead = 0;

    public ShuffledUnorderedTupleReader(ShuffleManager shuffleManager, Configuration conf,
                                     CompressionCodec codec, boolean ifileReadAhead, int ifileReadAheadLength, int ifileBufferSize,
                                     TezCounter inputRecordCounter)
            throws IOException {
        this.shuffleManager = shuffleManager;

        this.codec = codec;
        this.ifileReadAhead = ifileReadAhead;
        this.ifileReadAheadLength = ifileReadAheadLength;
        this.ifileBufferSize = ifileBufferSize;
        this.inputRecordCounter = inputRecordCounter;

        this.tupleClass = ConfigUtils.getIntermediateInputKeyClass(conf);

        this.keyIn = new DataInputBuffer();
        this.valIn = new DataInputBuffer();

        SerializationFactory serializationFactory = new SerializationFactory(conf);

        this.tupleSerializer = new TupleSerializer(Tuple2.class, new TypeSerializer[] {
                new StringSerializer(),
                new IntSerializer()
        });
        //this.valDeserializer.open(valIn);
    }

    // TODO NEWTEZ Maybe add an interface to check whether next will block.

    /**
     * Moves to the next key/values(s) pair
     *
     * @return true if another key/value(s) pair exists, false if there are no
     *         more.
     * @throws IOException
     *           if an error occurs
     */
    public boolean next() throws IOException {
        if (readNextFromCurrentReader()) {
            inputRecordCounter.increment(1);
            numRecordsRead++;
            return true;
        } else {
            boolean nextInputExists = moveToNextInput();
            while (nextInputExists) {
                if(readNextFromCurrentReader()) {
                    inputRecordCounter.increment(1);
                    numRecordsRead++;
                    return true;
                }
                nextInputExists = moveToNextInput();
            }
            LOG.info("Num Records read: " + numRecordsRead);
            return false;
        }
    }

    public Object getCurrentTuple(){
        return this.tuple;
    }

    /**
     * Tries reading the next key and value from the current reader.
     * @return true if the current reader has more records
     * @throws IOException
     */
    private boolean readNextFromCurrentReader() throws IOException {
        // Initial reader.
        if (this.currentReader == null) {
            return false;
        } else {
            boolean hasMore = this.currentReader.nextRawKey(keyIn);
            if (hasMore) {
                this.currentReader.nextRawValue(valIn);
                T reuse = tupleSerializer.createInstance();
                this.tuple = tupleSerializer.deserialize(reuse, new InputViewHelper(valIn));
                return true;
            }
            return false;
        }
    }

    /**
     * Moves to the next available input. This method may block if the input is not ready yet.
     * Also takes care of closing the previous input.
     *
     * @return true if the next input exists, false otherwise
     * @throws IOException
     * @throws InterruptedException
     */
    private boolean moveToNextInput() throws IOException {
        if (currentReader != null) { // Close the current reader.
            currentReader.close();
            currentFetchedInput.free();
        }
        try {
            currentFetchedInput = shuffleManager.getNextInput();
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting for next available input", e);
            throw new IOException(e);
        }
        if (currentFetchedInput == null) {
            return false; // No more inputs
        } else {
            currentReader = openIFileReader(currentFetchedInput);
            return true;
        }
    }

    public IFile.Reader openIFileReader(FetchedInput fetchedInput)
            throws IOException {
        if (fetchedInput.getType() == FetchedInput.Type.MEMORY) {
            MemoryFetchedInput mfi = (MemoryFetchedInput) fetchedInput;

            return new InMemoryReader(null, mfi.getInputAttemptIdentifier(),
                    mfi.getBytes(), 0, (int) mfi.getActualSize());
        } else {
            return new IFile.Reader(fetchedInput.getInputStream(),
                    fetchedInput.getCompressedSize(), codec, null, null, ifileReadAhead,
                    ifileReadAheadLength, ifileBufferSize);
        }
    }
}