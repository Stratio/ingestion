package com.stratio.ingestion.deserializer.compression;

import org.apache.commons.compress.compressors.CompressorStreamFactory;

public enum CompressionFormat {

    GZIP(CompressorStreamFactory.GZIP),
    BZIP2(CompressorStreamFactory.BZIP2),
    LZMA(CompressorStreamFactory.LZMA),
    PACK2000(CompressorStreamFactory.PACK200),
    XZ(CompressorStreamFactory.XZ),
    Z(CompressorStreamFactory.Z);

    private final String apacheFormat;

    CompressionFormat(final String apacheFormat) {
        this.apacheFormat = apacheFormat;
    }

    public String getApacheFormat() {
        return apacheFormat;
    }
}
