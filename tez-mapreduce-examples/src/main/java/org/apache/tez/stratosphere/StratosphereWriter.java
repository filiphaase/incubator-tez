package org.apache.tez.stratosphere;

import org.apache.tez.runtime.api.Writer;

import java.io.IOException;

/**
 * Created by filip on 21.05.14.
 */
public interface StratosphereWriter<T> extends Writer {

    public void write(T element) throws IOException;
}
