/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
