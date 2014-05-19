package org.apache.tez.stratosphere;

import org.apache.tez.runtime.api.Reader;

/**
 * Created by filip on 19.05.14.
 */
public interface StratosphereReader<T> extends Reader {

    public boolean hasNext() throws Exception;

    public T getNext() throws Exception;

}
