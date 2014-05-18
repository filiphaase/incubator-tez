package org.apache.tez.stratosphere;

import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.OutputBuffer;
import org.apache.http.MethodNotSupportedException;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by filip on 17.05.14.
 */
public class OutputViewHelper extends DataOutputStream implements DataOutputView{

    public OutputViewHelper(DataOutputStream out){
        super(out);
    }

    @Override
    public void skipBytesToWrite(int i) throws IOException {
        throw new RuntimeException("Method not implemented");
    }

    @Override
    public void write(DataInputView dataInputView, int i) throws IOException {
        throw new RuntimeException("Method not implemented");
    }
}
