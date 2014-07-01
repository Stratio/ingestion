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

import com.google.common.base.Charsets;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.flume.serialization.PositionTracker;
import org.apache.flume.serialization.ResettableInputStream;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;

public class ResettableDecompressInputStream extends ResettableInputStream {

    private final ResettableInputStream in;
    private CompressionFormat compressionFormat;
    private BufferedInputStream bufferedInputStream;
    private final PositionTracker positionTracker;

    private final int bufferLength;
    private ByteBuffer byteBuffer;
    private final CharsetDecoder charsetDecoder;
    private final CharBuffer charBuffer;

    private long position;

    public ResettableDecompressInputStream(final ResettableInputStream in, final CompressionFormat compressionFormat,
                                           final PositionTracker positionTracker, final int bufferLength) throws IOException {
        this(in, compressionFormat, positionTracker, bufferLength, Charsets.UTF_8);
    }

    public ResettableDecompressInputStream(final ResettableInputStream in, final CompressionFormat compressionFormat,
                                           final PositionTracker positionTracker, final int bufferLength, final Charset charset) throws IOException {
        this.compressionFormat = compressionFormat;
        this.in = in;
        this.positionTracker = positionTracker;

        if (in.tell() != 0) {
            throw new IllegalStateException("ResettableInputStream must be at position 0");
        }

        this.bufferLength = bufferLength;
        this.byteBuffer = ByteBuffer.allocate(8);
        this.charsetDecoder = charset.newDecoder();
        this.charBuffer = CharBuffer.allocate(1);

        in.mark();
        initStreams();

        seek(positionTracker.getPosition());
    }

    private void initStreams() throws IOException {
        try {
            if (bufferedInputStream != null) {
                bufferedInputStream.close();
            }
            final CompressorStreamFactory compressorStreamFactory = new CompressorStreamFactory();
            final ResettableInputStreamInputStream resettableInputStreamInputStream = new ResettableInputStreamInputStream(in);
            CompressorInputStream decompressedInputStream;
            //TODO: AUTO detection.
            //if (CompressionFormat.AUTO == compressionFormat) {
            //    decompressedInputStream = compressorStreamFactory.createCompressorInputStream(resettableInputStreamInputStream);
            //} else {
                decompressedInputStream = compressorStreamFactory.createCompressorInputStream(compressionFormat.getApacheFormat(), resettableInputStreamInputStream);
            //}
            this.bufferedInputStream = new BufferedInputStream(decompressedInputStream, bufferLength);
            this.position = 0;
        } catch (CompressorException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public int read() throws IOException {
        int result = bufferedInputStream.read();
        if (result == -1) {
            return -1;
        }
        position++;
        return result;
    }

    @Override
    public int read(final byte[] outBuffer, final int outOffset, final int outLength) throws IOException {
        int bytesRead = bufferedInputStream.read(outBuffer, outOffset, outLength);
        if (bytesRead == -1) {
            return -1;
        }
        position += bytesRead;
        return bytesRead;
    }

    @Override
    public int readChar() throws IOException {
        int totalBytesRead = 0;
        byteBuffer.clear();
        while (totalBytesRead < 4) {
            int bytesRead = bufferedInputStream.read(byteBuffer.array(), byteBuffer.position(), 1);
            if (bytesRead == -1) {
                return -1;
            }
            totalBytesRead += bytesRead;
            position += bytesRead;
            byteBuffer.position(totalBytesRead);
            byteBuffer.flip();
            charBuffer.clear();
            CoderResult coderResult = charsetDecoder.decode(byteBuffer, charBuffer, true);
            if (coderResult.isUnmappable()) {
                return '\uFFFD'; // Replacement character
                //XXX: coderResult.throwException();
            }
            charBuffer.flip();
            if (charBuffer.hasRemaining()) {
                return charBuffer.get();
            }
            byteBuffer.rewind();
            byteBuffer.compact();
        }
        return -1;
    }

    @Override
    public void mark() throws IOException {
        positionTracker.storePosition(position);
        bufferedInputStream.mark(bufferLength);
    }

    @Override
    public void reset() throws IOException {
        if (positionTracker.getPosition() > position - bufferLength) {
            bufferedInputStream.reset();
            position = positionTracker.getPosition();
        } else {
            resetFromScratch();
        }
    }

    private void resetFromScratch() throws IOException {
        in.seek(0);
        if (in.tell() != 0) {
            throw new IllegalStateException("ResettableInputStream should be at position 0");
        }
        initStreams();
        skip(positionTracker.getPosition());
    }

    private void skip(long n) throws IOException {
        while (n > 0) {
            long skipped = bufferedInputStream.skip(n);
            if (skipped > 0) {
                n -= skipped;
            }
        }
        position += n;
    }

    @Override
    public void seek(long l) throws IOException {
        if (position > l) {
            throw new UnsupportedOperationException("seek is only supported for forward positions");
        }
        skip(l - position);

    }

    @Override
    public long tell() throws IOException {
        return position;
    }

    @Override
    public void close() throws IOException {
        positionTracker.close();
        positionTracker.close();
        in.close();
        bufferedInputStream.close();
    }
}
