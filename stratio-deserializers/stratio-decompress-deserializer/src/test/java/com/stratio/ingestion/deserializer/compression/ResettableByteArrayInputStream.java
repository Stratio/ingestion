package com.stratio.ingestion.deserializer.compression;

import org.apache.flume.serialization.ResettableInputStream;

import java.io.IOException;

public class ResettableByteArrayInputStream extends ResettableInputStream {

    private final byte[] bytes;
    private int mark;
    private int position;
    private boolean isClosed;

    public ResettableByteArrayInputStream(final byte[] bytes) {
        this.bytes = bytes;
        this.position = 0;
        this.mark = 0;
        this.isClosed = false;
    }

    @Override
    public int read()throws IOException {
        ensureOpen();
        if (position >= bytes.length) {
            return -1;
        }
        position++;
        return bytes[position];
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        ensureOpen();
        if (position >= bytes.length) {
            return -1;
        }
        int bytesToRead = Math.min(length, bytes.length - position);
        System.arraycopy(bytes, position, buffer, offset, bytesToRead);
        position += bytesToRead;
        return bytesToRead;
    }

    @Override
    public int readChar() throws IOException {
        ensureOpen();
        throw new UnsupportedOperationException("UNSUPPORTED");

    }

    @Override
    public void mark() throws IOException {
        ensureOpen();
        mark = position;
    }

    @Override
    public void reset() throws IOException {
        ensureOpen();
        position = mark;
    }

    @Override
    public void seek(long l) throws IOException {
        ensureOpen();
        position = (int)l;
    }

    @Override
    public long tell() throws IOException {
        ensureOpen();
        return position;
    }

    private void ensureOpen() {
        if (isClosed) {
            throw new IllegalStateException("Closed stream");
        }
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            isClosed = true;
        }
    }
}
