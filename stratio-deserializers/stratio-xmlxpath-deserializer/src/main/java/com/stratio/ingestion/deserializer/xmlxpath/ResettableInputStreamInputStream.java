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
package com.stratio.ingestion.deserializer.xmlxpath;

import java.io.IOException;
import java.io.InputStream;

import org.apache.flume.serialization.ResettableInputStream;

/**
 * INTERNAL USE ONLY.
 * 
 * WARNING: Closing this InputStream will NOT close the underlying ResettableInputStream.
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
