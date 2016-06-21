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
package com.stratio.ingestion.sink.decision;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.decision.api.IStratioStreamingAPI;
import com.stratio.decision.api.StratioStreamingAPIFactory;
import com.stratio.decision.api.messaging.ColumnNameType;
import com.stratio.decision.api.messaging.ColumnNameValue;
import com.stratio.decision.commons.constants.ColumnType;
import com.stratio.decision.commons.exceptions.StratioEngineConnectionException;
import com.stratio.decision.commons.exceptions.StratioStreamingException;

public class StratioDecisionSink
        extends AbstractSink
        implements Configurable {

    private static final Logger log = LoggerFactory.getLogger(StratioDecisionSink.class);
    private static final int DEFAULT_BATCH_SIZE = 20;
    private static final String DEFAULT_ZOOKEEPER = "localhost:2181";
    private static final String DEFAULT_KAFKA = "localhost:9092";
    private static final String DEFAULT_TOPIC = "stratio_decision_data";
    private static final String DEFAULT_ZKPATH = "";
    private static final String CONF_BATCH_SIZE = "batchSize";
    private static final String ZOOKEEPER = "zookeeper";
    private static final String KAFKA = "kafka";
    private static final String STREAM_DEFINITION_FILE = "streamDefinitionFile";
    private static final String TOPIC = "topic";
    private static final String ZOOKEEPER_PATH = "zkPath";
    private static final String TOPIC_EVENT_HEADER = "_topic";

    private SinkCounter sinkCounter;
    private int batchsize;
    private IStratioStreamingAPI stratioStreamingAPI;
    private String zookeeper;
    private String kafka;
    private String streamName;
    private String topic= "";
    private String zkPath= "";
    private List<StreamField> streamFields;

    public StratioDecisionSink() {
        super();
    }

    public void configure(Context context) {
        this.batchsize = context.getInteger(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE);
        this.sinkCounter = new SinkCounter(this.getName());
        this.zookeeper = context.getString(ZOOKEEPER, DEFAULT_ZOOKEEPER);
        this.kafka = context.getString(KAFKA, DEFAULT_KAFKA);
        this.zkPath = context.getString(ZOOKEEPER_PATH,DEFAULT_ZKPATH);
        //if (context.getString(TOPIC) != null)
        //    this.topic= context.getString(TOPIC);
        this.topic = context.getString(TOPIC, "");

        log.info("Configuring Stratio Decision Sink: {zookeeper= " + this.zookeeper + ", kafka= "
                + this.kafka + ", topic= " + this.topic
                + ", batchSize= " + this.batchsize + ", sinkCounter= " + this.sinkCounter + "}");
        //else
        //this.topic= "";

        String columnDefinitionFile = context.getString(STREAM_DEFINITION_FILE);
        com.stratio.ingestion.sink.decision.StreamDefinitionParser parser = new StreamDefinitionParser(readJsonFromFile(new File(columnDefinitionFile)));
        StreamDefinition theStreamDefinition = parser.parse();
        this.streamName = theStreamDefinition.getStreamName();
        this.streamFields = theStreamDefinition.getFields();

        try {
            this.stratioStreamingAPI = StratioStreamingAPIFactory.create()
                    .withQuorumConfig(kafka, zookeeper,zkPath)
                    .init();
        } catch (StratioEngineConnectionException e) {
            throw new StratioDecisionSinkException(e);
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

                    if (event.getHeaders().containsKey(TOPIC_EVENT_HEADER)) {
                        // If we've defined the _topic header using in the event, we send the data to this topic
                        stratioStreamingAPI.insertData(this.streamName, columnNameValueList,
                                getEventDefinedTopicName(event.getHeaders().get(TOPIC_EVENT_HEADER)), false);

                    } else if (!this.topic.isEmpty())   {
                        // If we've specified a topic in the properties file we send the data to that topic
                        stratioStreamingAPI.insertData(this.streamName, columnNameValueList, this.topic, false);
                    }   else    {
                        // In other case we send the data to default topic, don't specifying any topic name
                        stratioStreamingAPI.insertData(this.streamName, columnNameValueList);
                    }
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
                throw new StratioDecisionSinkException(t);
            }
        } finally {
            transaction.close();
        }
        return status;
    }

    private String getEventDefinedTopicName(String eventTopicName)  {
//        return DEFAULT_TOPIC + "_" + eventTopicName;
        return eventTopicName;
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
            throw new StratioDecisionSinkException(e);
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
