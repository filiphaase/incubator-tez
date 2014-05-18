package org.apache.tez.stratosphere;

import eu.stratosphere.core.memory.DataInputView;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Created by filip on 17.05.14.
 */
public class InputViewHelper implements DataInputView{

    DataInput in;
    public InputViewHelper(DataInput in) {
        this.in = in;
    }

    @Override
    public void skipBytesToRead(int numBytes) throws IOException {
        //while (numBytes > 0) {
        skipBytes(numBytes);
          //  numBytes -= skipped;
        //}
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
        in.readFully(bytes);
    }

    @Override
    public void readFully(byte[] bytes, int i, int i2) throws IOException {
        in.readFully(bytes, i, i2);
    }

    @Override
    public int skipBytes(int i) throws IOException {
        return in.skipBytes(i);
    }

    @Override
    public boolean readBoolean() throws IOException {
        return in.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        return in.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return in.readUnsignedByte();
    }

    @Override
    public short readShort() throws IOException {
        return in.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return in.readUnsignedShort();
    }

    @Override
    public char readChar() throws IOException {
        return readChar();
    }

    @Override
    public int readInt() throws IOException {
        return in.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return in.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        return in.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        return in.readDouble();
    }

    @Override
    public String readLine() throws IOException {
        return in.readLine();
    }

    @Override
    public String readUTF() throws IOException {
        return in.readUTF();
    }
}
