package com.stratio.ingestion.deserializer.xmlxpath;

import java.io.IOException;
import java.io.InputStream;

import org.apache.flume.serialization.ResettableInputStream;

class ResettableInputStreamInputStream extends InputStream {

  private final ResettableInputStream in;

  public ResettableInputStreamInputStream(final ResettableInputStream in) {
    this.in = in;
  }

  @Override public int read() throws IOException {
    return in.read();
  }

}
