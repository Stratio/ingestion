package com.stratio.ingestion.deserializer.compression;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.IOUtils;
import org.apache.flume.serialization.ResettableInputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayOutputStream;
import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.OutputStream;

import static org.fest.assertions.Assertions.assertThat;

@RunWith(JUnit4.class)
public class ResettableDecompressInputStreamTest {

    @Test
    public void readChar() throws IOException, CompressorException {

        final String TEXT = "ONE\nTWO\nTHREE\n©£¢\n";

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final OutputStream outputStream = new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.GZIP, byteArrayOutputStream);
        IOUtils.write(TEXT, outputStream);
        outputStream.flush();
        outputStream.close();

        ResettableInputStream resettableInputStream = new ResettableByteArrayInputStream(byteArrayOutputStream.toByteArray());

        ResettableDecompressInputStream resettableDecompressInputStream = new ResettableDecompressInputStream(
            resettableInputStream, CompressionFormat.GZIP, new TransientPositionTracker("DUMMY")
        );

        CharArrayWriter charArrayWriter = new CharArrayWriter();
        int c = -1;
        while ((c = resettableDecompressInputStream.readChar()) != -1) {
            charArrayWriter.append((char)c);
        }

        assertThat(charArrayWriter.toCharArray()).isEqualTo(TEXT.toCharArray());
    }

}
