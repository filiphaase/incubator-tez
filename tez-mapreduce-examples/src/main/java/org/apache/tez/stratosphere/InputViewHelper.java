package org.apache.tez.stratosphere;

import eu.stratosphere.core.memory.DataInputView;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Created by filip on 17.05.14.
 */
public class InputViewHelper extends DataInputStream implements DataInputView{

    public InputViewHelper(DataInputStream in) {
        super(in);
    }

    @Override
    public void skipBytesToRead(int numBytes) throws IOException {
        skipBytes(numBytes);
    }

}
