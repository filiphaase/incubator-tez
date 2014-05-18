package org.apache.tez.stratosphere;

import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.OutputBuffer;
import org.apache.http.MethodNotSupportedException;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by filip on 17.05.14.
 */
public class OutputViewHelper implements DataOutputView{

    private DataOutput out;

    public OutputViewHelper(){
        this(new DataOutputBuffer(4096));
    }
    public OutputViewHelper(DataOutput out){
        this.out = out;
    }

    public byte[] getWrittenData(){
        return ((DataOutputBuffer)out).getData();
    }

    @Override
    public void skipBytesToWrite(int i) throws IOException {
        throw new IOException("Method not supported");
    }

    @Override
    public void write(DataInputView dataInputView, int i) throws IOException {
        throw new IOException("Method not supported");
    }

    @Override
    public void write(int i) throws IOException {
        out.write(i);
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        out.write(bytes);
    }

    @Override
    public void write(byte[] bytes, int i, int i2) throws IOException {
        out.write(bytes, i, i2);
    }

    @Override
    public void writeBoolean(boolean b) throws IOException {
        out.writeBoolean(b);
    }

    @Override
    public void writeByte(int i) throws IOException {
        out.writeByte(i);
    }

    @Override
    public void writeShort(int i) throws IOException {
        out.writeShort(i);
    }

    @Override
    public void writeChar(int i) throws IOException {
        out.writeChar(i);
    }

    @Override
    public void writeInt(int i) throws IOException {
        out.writeInt(i);
    }

    @Override
    public void writeLong(long l) throws IOException {
        out.writeLong(l);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        out.writeFloat(v);
    }

    @Override
    public void writeDouble(double v) throws IOException {
        out.writeDouble(v);
    }

    @Override
    public void writeBytes(String s) throws IOException {
        out.writeBytes(s);
    }

    @Override
    public void writeChars(String s) throws IOException {
        out.writeChars(s);
    }

    @Override
    public void writeUTF(String s) throws IOException {
        out.writeUTF(s);
    }
}
