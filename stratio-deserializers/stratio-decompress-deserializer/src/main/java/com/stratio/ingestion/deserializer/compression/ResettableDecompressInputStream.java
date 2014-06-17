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

    private ByteBuffer byteBuffer;
    private final CharsetDecoder charsetDecoder;
    private final CharBuffer charBuffer;

    private long position;

    public ResettableDecompressInputStream(final ResettableInputStream in, final CompressionFormat compressionFormat,
                                           final PositionTracker positionTracker) throws IOException {
        this(in, compressionFormat, positionTracker, Charsets.UTF_8);
    }

    public ResettableDecompressInputStream(final ResettableInputStream in, final CompressionFormat compressionFormat,
                                           final PositionTracker positionTracker, final Charset charset) throws IOException {
        this.compressionFormat = compressionFormat;
        this.in = in;
        this.positionTracker = positionTracker;

        if (in.tell() != 0) {
            throw new IllegalStateException("ResettableInputStream must be at position 0");
        }

        in.mark();
        initStreams();

        seek(positionTracker.getPosition());

        this.byteBuffer = ByteBuffer.allocate(4);
        this.charsetDecoder = charset.newDecoder();
        this.charBuffer = CharBuffer.allocate(1);
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
            this.bufferedInputStream = new BufferedInputStream(decompressedInputStream, 1024 * 10);
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
        bufferedInputStream.mark(4);
        byteBuffer.clear();
        int totalBytesRead = 0;
        while (byteBuffer.position() < 3) {
            int bytesRead = bufferedInputStream.read(byteBuffer.array(), byteBuffer.position(), byteBuffer.capacity() - byteBuffer.position());
            if (bytesRead == -1) {
                break;
            }
            totalBytesRead += bytesRead;
            byteBuffer.position(totalBytesRead);
        }
        if (totalBytesRead == 0) {
            return -1;
        }
        byteBuffer.flip();
        charBuffer.clear();
        CoderResult coderResult = charsetDecoder.decode(byteBuffer, charBuffer, true);
        if (coderResult.isMalformed() || coderResult.isUnmappable()) {
            return '\uFFFD'; // Replacement character
            //XXX: bufferedInputStream.reset();
            //XXX: coderResult.throwException();
        }
        charBuffer.flip();
        final int toSkip = byteBuffer.position();
        position += toSkip;
        if (coderResult.isOverflow()) {
            bufferedInputStream.reset();
            bufferedInputStream.skip(toSkip);
        }
        if (charBuffer.hasRemaining()) {
            return charBuffer.get();
        } else {
            return -1;
        }
    }

    @Override
    public void mark() throws IOException {
        positionTracker.storePosition(position);
    }

    @Override
    public void reset() throws IOException {
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
        in.close();
        bufferedInputStream.close();
    }
}
