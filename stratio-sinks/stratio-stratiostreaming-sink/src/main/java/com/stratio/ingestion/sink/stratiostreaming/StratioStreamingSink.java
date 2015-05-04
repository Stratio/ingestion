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
package com.stratio.ingestion.sink.stratiostreaming;

import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.api.StratioStreamingAPIFactory;
import com.stratio.streaming.api.messaging.ColumnNameType;
import com.stratio.streaming.api.messaging.ColumnNameValue;
import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.exceptions.StratioEngineConnectionException;
import com.stratio.streaming.commons.exceptions.StratioStreamingException;

import org.apache.commons.io.IOUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;

import java.io.File;
import java.io.FileInputStream;
import java.util.*;

public class StratioStreamingSink
        extends AbstractSink
        implements Configurable {

    private static final int DEFAULT_BATCH_SIZE = 20;
    private static final String DEFAULT_ZOOKEEPER = "localhost:2181";
    private static final String DEFAULT_KAFKA = "localhost:9092";
    private static final String CONF_BATCH_SIZE = "batchSize";
    private static final String ZOOKEEPER = "zookeeper";
    private static final String KAFKA = "kafka";
    private static final String STREAM_DEFINITION_FILE = "streamDefinitionFile";

    private SinkCounter sinkCounter;
    private int batchsize;
    private IStratioStreamingAPI stratioStreamingAPI;
    private String zookeeper;
    private String kafka;
    private String streamName;
    private List<StreamField> streamFields;

    public StratioStreamingSink() {
        super();
    }

    public void configure(Context context) {
        this.batchsize = context.getInteger(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE);
        this.sinkCounter = new SinkCounter(this.getName());
        this.zookeeper = context.getString(ZOOKEEPER, DEFAULT_ZOOKEEPER);
        this.kafka = context.getString(KAFKA, DEFAULT_KAFKA);
        String columnDefinitionFile = context.getString(STREAM_DEFINITION_FILE);
        StreamDefinitionParser parser = new StreamDefinitionParser(readJsonFromFile(new File(columnDefinitionFile)));
        StreamDefinition theStreamDefinition = parser.parse();
        this.streamName = theStreamDefinition.getStreamName();
        this.streamFields = theStreamDefinition.getFields();

        try {
            this.stratioStreamingAPI = StratioStreamingAPIFactory.create()
                    .withServerConfig(kafka, zookeeper)
                    .init();
        } catch (StratioEngineConnectionException e) {
            throw new StratioStreamingSinkException(e);
        }
    }

    @Override public synchronized void start() {
        super.start();
        createStream();
        this.sinkCounter.start();
    }

    private void createStream() {
        try {
            String streamName = this.streamName;
            List<ColumnNameType> columnList = new ArrayList<ColumnNameType>();
            for (StreamField streamField : this.streamFields) {
                ColumnNameType streamColumn = new ColumnNameType(streamField.getName(),
                        parseStreamField(streamField.getType()));
                columnList.add(streamColumn);
            }
            stratioStreamingAPI.createStream(streamName, columnList);
        } catch (StratioStreamingException e) {
            e.printStackTrace();
        }
    }

    public Status process() throws EventDeliveryException {
        Status status = Status.BACKOFF;
        Transaction transaction = this.getChannel().getTransaction();
        try {
            transaction.begin();
            List<Event> eventList = this.takeEventsFromChannel(this.getChannel());
            status = Status.READY;
            if (!eventList.isEmpty()) {
                if (eventList.size() == this.batchsize) {
                    this.sinkCounter.incrementBatchCompleteCount();
                } else {
                    this.sinkCounter.incrementBatchUnderflowCount();
                }
                for (Event event : eventList) {
                    List<ColumnNameValue> columnNameValueList = getColumnNameValueListFromEvent(event);
                    stratioStreamingAPI.insertData(this.streamName, columnNameValueList);
                }
                this.sinkCounter.addToEventDrainSuccessCount(eventList.size());
            } else {
                this.sinkCounter.incrementBatchEmptyCount();
            }
            transaction.commit();
            status = Status.READY;
        } catch (ChannelException e) {
            e.printStackTrace();
            transaction.rollback();
            status = Status.BACKOFF;
            this.sinkCounter.incrementConnectionFailedCount();
        } catch (Throwable t) {
            t.printStackTrace();
            transaction.rollback();
            status = Status.BACKOFF;
            if (t instanceof Error) {
                throw new StratioStreamingSinkException(t);
            }
        } finally {
            transaction.close();
        }
        return status;
    }

    private List<ColumnNameValue> getColumnNameValueListFromEvent(Event event) {
        List<ColumnNameValue> columnNameValues = new ArrayList<ColumnNameValue>();
        Map<String, String> headers = event.getHeaders();
        for (StreamField field : streamFields) {
            String fieldContent = headers.get(field.getName());
            columnNameValues.add(new ColumnNameValue(field.getName(), fieldContent));
        }
        return columnNameValues;
    }

    private List<Event> takeEventsFromChannel(Channel channel) {
        List<Event> events = new ArrayList<Event>();
        for (int i = 0; i < this.batchsize; i++) {
            this.sinkCounter.incrementEventDrainAttemptCount();
            events.add(channel.take());
        }
        events.removeAll(Collections.singleton(null));
        return events;
    }

    private static final String readJsonFromFile(File file) {
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(file);
            return IOUtils.toString(inputStream, "UTF-8");
        } catch (Exception e) {
            throw new StratioStreamingSinkException(e);
        }
    }

    private ColumnType parseStreamField(String field) {
        try {
            return ColumnType.valueOf(field.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException ex) {
            //TODO: log something
        }
        return ColumnType.STRING;
    }
}
