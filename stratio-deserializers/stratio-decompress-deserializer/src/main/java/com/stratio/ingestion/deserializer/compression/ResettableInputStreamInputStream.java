package com.stratio.ingestion.deserializer.compression;

import org.apache.flume.serialization.ResettableInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * INTERNAL USE ONLY.
 *
 * WARNING: Closing this InputStream will NOT close the underlying
 *          ResettableInputStream.
 *
 * WARNING: reset/mark does NOT work.
 *
 */
class ResettableInputStreamInputStream extends InputStream {

    private final ResettableInputStream in;

    public ResettableInputStreamInputStream(final ResettableInputStream in) {
        this.in = in;
    }


    @Override
    public int read() throws IOException {
        return in.read();
    }

    @Override
    public int read(final byte[] buffer, final int offset, final int length) throws IOException {
        return in.read(buffer, offset, length);
    }

}
