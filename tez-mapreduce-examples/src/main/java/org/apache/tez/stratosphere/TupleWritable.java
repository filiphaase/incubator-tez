package org.apache.tez.stratosphere;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.base.IntSerializer;
import eu.stratosphere.api.common.typeutils.base.StringSerializer;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.runtime.TupleSerializer;
import org.apache.hadoop.io.WritableComparable;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by filip on 17.05.14.
 */
public class TupleWritable implements WritableComparable<TupleWritable> {
    private Tuple2<String, Integer> tuple;
    private TupleSerializer<Tuple2<String, Integer>> serializer;

    public Tuple2 getTuple(){
        return tuple;
    }

    public TupleWritable(){
        this.serializer = new TupleSerializer(Tuple2.class, new TypeSerializer[] {
                new StringSerializer(),
                new IntSerializer()});
    }

    public TupleWritable(Tuple2<String, Integer> tuple){
        this.tuple = tuple;
        this.serializer = new TupleSerializer(Tuple2.class, new TypeSerializer[] {
                new StringSerializer(),
                new IntSerializer()});
    }

    @Override
    public void write(DataOutput out) throws IOException {
        OutputViewHelper outputView = new OutputViewHelper(out);
        this.serializer.serialize(tuple, outputView);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.tuple = serializer.createInstance();
        this.serializer.deserialize(tuple, new InputViewHelper(in));
    }

    @Override
    public int compareTo(TupleWritable tupleWritable) {
        return this.tuple.f0.compareTo(((Tuple2<String,Integer> )tupleWritable.getTuple()).f0);
    }
}