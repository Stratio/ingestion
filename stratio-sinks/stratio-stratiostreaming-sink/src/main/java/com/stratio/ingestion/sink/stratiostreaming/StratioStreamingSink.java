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
import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.exceptions.StratioStreamingException;
import com.stratio.streaming.messaging.ColumnNameType;
import com.stratio.streaming.messaging.ColumnNameValue;
import org.apache.commons.io.IOUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.conf.Configurables;


import java.io.File;
import java.io.FileInputStream;
import java.util.*;

public class StratioStreamingSink
        extends AbstractSink
        implements Configurable {

    //Required fields
    private static final String ZOOKEEPER_HOST = "zookeeperHost";
    private static final String ZOOKEEPER_PORT = "zookeeperPort";
    private static final String KAFKA_HOST = "kafkaHost";
    private static final String KAFKA_PORT = "kafkaPort";
    private static final String STREAM_DEFINITION_FILE = "streamDefinitionFile";

    //Optional fields
    private static final String CONF_BATCH_SIZE = "batchSize";
    private static final String STREAM_NAME = "streamName";
    private static final String DYNAMIC = "dynamic";
    private static final String DYNAMIC_STREAM = "dynamicStreamHeader";

    private static final int DEFAULT_BATCH_SIZE = 20;
    private SinkCounter sinkCounter;
    private int batchsize;
    private IStratioStreamingAPI stratioStreamingAPI;
    private String zookeeperHost;
    private Integer zookeeperPort;
    private String kafkaHost;
    private Integer kafkaPort;
    private String dynamicStreamHeader;
    private String streamName;
    private List<StreamField> streamFields;
    private HashSet<String> streamsCreated = new HashSet<String>();
    private boolean dynamicStreamCreationEnabled = false;

    public StratioStreamingSink() {
        super();
    }

    public void configure(Context context) {
        try {
            this.dynamicStreamCreationEnabled = checkDynamicStreamCreationEnabled(context);
            ensureRequiredFieldsNonNull(context);
            ensureOptionalFieldsDefinedProperly(context);
            this.sinkCounter = new SinkCounter(this.getName());
            this.batchsize = context.getInteger(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE);
            this.zookeeperHost = context.getString(ZOOKEEPER_HOST);
            this.zookeeperPort = context.getInteger(ZOOKEEPER_PORT);
            this.kafkaHost = context.getString(KAFKA_HOST);
            this.kafkaPort = context.getInteger(KAFKA_PORT);
            String columnDefinitionFile = context.getString(STREAM_DEFINITION_FILE);
            StreamDefinitionParser parser = new StreamDefinitionParser(readJsonFromFile(new File(columnDefinitionFile)));
            StreamDefinition theStreamDefinition = parser.parse();
            this.streamFields = theStreamDefinition.getFields();
            this.stratioStreamingAPI = StratioStreamingAPIFactory.
                    create().
                    initializeWithServerConfig(kafkaHost,
                            kafkaPort,
                            zookeeperHost,
                            zookeeperPort);
            if (!dynamicStreamCreationEnabled) {
                this.streamName = context.getString(STREAM_NAME);
                createStream(this.streamName);
            } else {
                this.dynamicStreamHeader = context.getString(DYNAMIC_STREAM);
            }
        } catch (Exception e) {
            throw new StratioStreamingSinkException(e);
        }
    }


    private void ensureRequiredFieldsNonNull(Context context) {
        Configurables.ensureRequiredNonNull(context, ZOOKEEPER_HOST);
        Configurables.ensureRequiredNonNull(context, ZOOKEEPER_PORT);
        Configurables.ensureRequiredNonNull(context, KAFKA_HOST);
        Configurables.ensureRequiredNonNull(context, KAFKA_PORT);
        Configurables.ensureRequiredNonNull(context, STREAM_DEFINITION_FILE);
    }

    private void ensureOptionalFieldsDefinedProperly(Context context) {
        if (dynamicStreamCreationEnabled)
            if (!dynamicStreamNameIsDefined(context))
                throw new IllegalArgumentException("Required parameter " + DYNAMIC_STREAM
                        + " must exist and may not be null");
    }

    private boolean checkDynamicStreamCreationEnabled(Context context) {
        return context.getParameters().containsKey(DYNAMIC)
                && context.getParameters().get(DYNAMIC).equals("true");
    }

    private boolean dynamicStreamNameIsDefined(Context context) {
        return context.getParameters().containsKey(DYNAMIC_STREAM)
                && context.getParameters().get(DYNAMIC_STREAM) != null;
    }

    private void createStream(String streamName) {
        try {
            List<ColumnNameType> columnList = new ArrayList<ColumnNameType>();
            for (StreamField streamField: this.streamFields) {
                ColumnNameType streamColumn = new ColumnNameType(streamField.getName(), parseStreamField(streamField.getType()));
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
                    String theStreamName = this.streamName;
                    List<ColumnNameValue> columnNameValueList = getColumnNameValueListFromEvent(event);
                    if (dynamicStreamCreationEnabled) {
                        Map<String, String> headers = event.getHeaders();
                        theStreamName = headers.get(dynamicStreamHeader);
                        if (!streamsCreated.contains(theStreamName)) {
                            createStream(theStreamName);
                            streamsCreated.add(theStreamName);
                        }
                    }
                    stratioStreamingAPI.insertData(theStreamName, columnNameValueList);
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
        for (StreamField field: streamFields) {
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
        ColumnType columnType;
        switch (field) {
            case "string" : columnType =  ColumnType.STRING;
                            break;
            case "boolean" : columnType = ColumnType.BOOLEAN;
                            break;
            case "double" : columnType = ColumnType.DOUBLE;
                            break;
            case "integer" : columnType = ColumnType.INTEGER;
                            break;
            case "long" : columnType = ColumnType.LONG;
                            break;
            case "float" : columnType = ColumnType.FLOAT;
                            break;
            default: columnType = ColumnType.STRING;
                      break;
        }
        return columnType;
    }
}
