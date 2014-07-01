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


import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.serialization.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Locale;

public class DecompressDeserializer implements EventDeserializer {

    private static final Logger log = LoggerFactory.getLogger(DecompressDeserializer.class);

    private static final String CONF_FORMAT = "format";
    private static final String CONF_DESERIALIZER = "deserializer";
    private static final String CONF_CHARSET = "charset";
    private static final String CONF_BUFFER_LENGTH = "bufferLength";
    private static final String CONF_TRACKER_FILE = "trackerFile";
    private static final String DEFAULT_DESERIALIZER = "LINE";
    private static final String DEFAULT_CHARSET = "UTF-8";
    private static final int DEFAULT_BUFFER_LENGTH = 1 * 1024 * 1024;

    private final ResettableDecompressInputStream decompressInputStream;
    private final EventDeserializer eventDeserializer;
    private boolean isOpen;

    DecompressDeserializer(Context context, ResettableInputStream resettableInputStream) throws IOException {
        final String compressionFormatString = context.getString(CONF_FORMAT);
        if (compressionFormatString == null) {
            throw new ConfigurationException(String.format("%s option must be present", CONF_FORMAT));
        }
        final CompressionFormat compressionFormat = CompressionFormat.valueOf(compressionFormatString.toUpperCase(Locale.ENGLISH));

        final String trackerFilePath = context.getString(CONF_TRACKER_FILE);
        if (trackerFilePath == null) {
            throw new ConfigurationException(String.format("%s option must be present", CONF_TRACKER_FILE));
        }
        File metaFile = new File(trackerFilePath);
        PositionTracker positionTracker = DurablePositionTracker.getInstance(metaFile, "DUMMY");
        if (!positionTracker.getTarget().equals("DUMMY")) {
            positionTracker.close();
            if (metaFile.exists() && !metaFile.delete()) {
                throw new IOException("Unable to delete old meta file " + metaFile);
            }
            log.debug("Tracker file deleted");
            positionTracker = DurablePositionTracker.getInstance(metaFile, "DUMMY");
        }

        final String deserializerType = context.getString(CONF_DESERIALIZER, DEFAULT_DESERIALIZER);
        final Context deserializerContext = new Context(context.getSubProperties(CONF_DESERIALIZER + "."));

        final String charsetString = context.getString(CONF_CHARSET, DEFAULT_CHARSET);
        final Charset charset = Charset.forName(charsetString);

        this.decompressInputStream = new ResettableDecompressInputStream(resettableInputStream, compressionFormat,
                positionTracker, context.getInteger(CONF_BUFFER_LENGTH, DEFAULT_BUFFER_LENGTH), charset);
        this.eventDeserializer = EventDeserializerFactory.getInstance(deserializerType, deserializerContext, decompressInputStream);
        this.isOpen = true;
    }

    @Override
    public Event readEvent() throws IOException {
        ensureOpen();
        return eventDeserializer.readEvent();
    }

    @Override
    public List<Event> readEvents(int numEvents) throws IOException {
        ensureOpen();
        return eventDeserializer.readEvents(numEvents);
    }

    @Override
    public void mark() throws IOException {
        ensureOpen();
        this.eventDeserializer.mark();
        //this.decompressInputStream.mark();
    }

    @Override
    public void reset() throws IOException {
        ensureOpen();
        this.eventDeserializer.reset();
        //this.decompressInputStream.reset();
    }

    @Override
    public void close() throws IOException {
        if (isOpen) {
            isOpen = false;
            decompressInputStream.close();
            //FIXME: Closing the event serializer will cause a reset()
            //     which will break our ResettableDecompressInputStream.
            //eventDeserializer.close();
        }
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw new IllegalStateException("Serializer has been closed");
        }
    }

    public static class Builder implements EventDeserializer.Builder {

        @Override
        public EventDeserializer build(Context context, ResettableInputStream resettableInputStream) {
            try {
                return new DecompressDeserializer(context, resettableInputStream);
            } catch (IOException ex) {
                throw new RuntimeException(ex); //TODO: Throw something more sensible here.
            }
        }
    }

}
