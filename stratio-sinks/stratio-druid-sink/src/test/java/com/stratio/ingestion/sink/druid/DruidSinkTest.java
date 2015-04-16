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
package com.stratio.ingestion.sink.druid;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;

/**
 * Created by eambrosio on 30/03/15.
 */
public class DruidSinkTest {

    private Channel channel;
    private DruidSink druidSink;

    @Before
    public void setup() {
        //        Context channelContext = new Context();
        //        channelContext.put("checkpointDir","data/check");
        //        channelContext.put("dataDirs","data/data");
        //        channelContext.put("capacity","1000");
        //        channelContext.put("transactionCapacity","100");
        //        channelContext.put("checkpointInterval","300");
        //        channel = new FileChannel();
        Context channelContext = new Context();
        channelContext.put("capacity", "10000");
        channelContext.put("transactionCapacity", "5000");
        channel = new MemoryChannel();
        channel.setName("junitChannel");
        Configurables.configure(channel, channelContext);
        channel.start();

        druidSink = new DruidSink();
        druidSink.setChannel(channel);
        druidSink.configure(getMockContext());
        druidSink.start();
    }

    @Test
    @Ignore
    public void processValidEvents() throws EventDeliveryException {
        Transaction tx = channel.getTransaction();
        tx.begin();
        getNTrackerEvents(1000);
        tx.commit();
        tx.close();
        for (int i = 0; i < 1; i++) {
            druidSink.process();
        }

        tx = channel.getTransaction();
        tx.begin();
        Assertions.assertThat(channel.take()).isNull();
    }

    @Test
    @Ignore
    public void process500KValidEvents() throws EventDeliveryException {
        for (int i = 0; i < 10; i++) {
            processValidEvents();
        }
    }

    private void getNEvents(int numEvents, TimeUnit timeUnit) {
        for (int i = 0; i < numEvents; i++) {
            channel.put(getEvent(getOffset(timeUnit)));
        }
    }

    private void getNTrackerEvents(int numEvents) {
        for (int i = 0; i < numEvents; i++) {
            channel.put(getTrackerEvent());
        }
    }

    private long getOffset(TimeUnit timeUnit) {
        long offset = 0;
        switch (timeUnit) {
        case MILLISECONDS:
            offset = 1;
            break;
        case SECONDS:
            offset = 1000;
            break;
        case MINUTES:
            offset = 1000 * 60;
            break;
        case HOURS:
            offset = 1000 * 60 * 60;
            break;
        case DAYS:
            offset = 1000 * 60 * 60 * 24;
            break;
        default:
            offset = 0;
            break;
        }
        return offset;
    }

    private Event getTrackerEvent() {
        Random random = new Random();
        String[] users = new String[] { "user1@santander.com", "user2@santander.com", "user3@santander.com",
                "user4@santander.com" };
        String[] isoCode = new String[] { "DE", "ES", "US", "FR" };
        TimeUnit[] offset = new TimeUnit[] { TimeUnit.DAYS, TimeUnit.HOURS, TimeUnit.SECONDS };
        ObjectNode jsonBody = new ObjectNode(JsonNodeFactory.instance);
        Map<String, String> headers;
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = null;
        final String fileName = "/trackerSample" + random.nextInt(4) + ".json";
        try {
            jsonNode = mapper.readTree(getClass().getResourceAsStream(fileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        headers = mapper.convertValue(jsonNode, Map.class);
        headers.put("timestamp", String.valueOf(new Date().getTime() + getOffset(offset[random.nextInt(3)]) * random
                .nextInt(100)));
        headers.put("santanderID", users[random.nextInt(4)]);
        headers.put("isoCode", isoCode[random.nextInt(4)]);

        return EventBuilder.withBody(jsonBody.toString().getBytes(Charsets.UTF_8), headers);
    }

    private Event getEvent(long offset) {
        ObjectNode jsonBody = new ObjectNode(JsonNodeFactory.instance);
        jsonBody.put("field1", "foo");
        jsonBody.put("field2", 32);
        jsonBody.put("timestamp", String.valueOf(new Date().getTime()));

        Map<String, String> headers = new HashMap<String, String>();
        headers.put("field3", "bar"); // Overwrites the value defined in JSON body
        headers.put("field4", "64");
        headers.put("field5", "true");
        headers.put("field6", "1.0");
        headers.put("field7", "11");
        final long l = new Date().getTime();
        headers.put("timestamp", String.valueOf(l + offset));

        headers.put("myString2", "baz");

        return EventBuilder.withBody(jsonBody.toString().getBytes(Charsets.UTF_8), headers);
    }

    private Context getMockContext() {
        Map<String, String> mapProperties = loadProperties("/context.properties");
        Context context = new Context(mapProperties);
        return context;
    }

    private Map<String, String> loadProperties(String file) {
        Properties properties = new Properties();
        try {
            properties.load(getClass().getResourceAsStream(file));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Maps.fromProperties(properties);
    }

}