package org.apache.tez.stratosphere;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableFactories;

import eu.stratosphere.core.io.InputSplit;

/**
 * Created by filip on 19.05.14.
 *
 * COPIED from Stratosphere.hadoopcompatibility.HadoopInputSplitWrapper
 * which is somehow not in the jars
 */
public class HadoopInputSplitWrapper implements InputSplit{

        public transient org.apache.hadoop.mapred.InputSplit hadoopInputSplit;
        private int splitNumber;
        private String hadoopInputSplitTypeName;


        public org.apache.hadoop.mapred.InputSplit getHadoopInputSplit() {
            return hadoopInputSplit;
        }

        public HadoopInputSplitWrapper() {
            super();
        }


        public HadoopInputSplitWrapper(org.apache.hadoop.mapred.InputSplit hInputSplit) {
            this.hadoopInputSplit = hInputSplit;
            this.hadoopInputSplitTypeName = hInputSplit.getClass().getName();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(splitNumber);
            out.writeUTF(hadoopInputSplitTypeName);
            hadoopInputSplit.write(out);
        }

        @Override
        public void read(DataInput in) throws IOException {
            this.splitNumber=in.readInt();
            this.hadoopInputSplitTypeName = in.readUTF();
            if(hadoopInputSplit == null) {
                try {
                    Class<? extends org.apache.hadoop.io.Writable> inputSplit =
                            Class.forName(hadoopInputSplitTypeName).asSubclass(org.apache.hadoop.io.Writable.class);
                    this.hadoopInputSplit = (org.apache.hadoop.mapred.InputSplit) WritableFactories.newInstance( inputSplit );
                }
                catch (Exception e) {
                    throw new RuntimeException("Unable to create InputSplit", e);
                }
            }
            this.hadoopInputSplit.readFields(in);
        }

        @Override
        public int getSplitNumber() {
            return this.splitNumber;
        }

        public void setSplitNumber(int splitNumber) {
            this.splitNumber = splitNumber;
        }

        public void setHadoopInputSplit(
                org.apache.hadoop.mapred.InputSplit hadoopInputSplit) {
            this.hadoopInputSplit = hadoopInputSplit;
        }

}
