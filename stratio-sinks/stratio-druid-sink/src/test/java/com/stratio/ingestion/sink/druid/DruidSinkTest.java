/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.ingestion.sink.druid;

import static org.fest.assertions.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.Test;

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

    public void setup() {
        Context channelContext = new Context();
        channelContext.put("capacity", "10000");
        channelContext.put("transactionCapacity", "200");

        channel = new MemoryChannel();
        channel.setName("junitChannel");
        Configurables.configure(channel, channelContext);

        druidSink = new DruidSink();
        druidSink.setChannel(channel);

        druidSink.configure(getMockContext());
        channel.start();
        druidSink.start();
    }

    @Test
    public void parseValidEvents() throws Exception {
        druidSink = new DruidSink();
        List<Event> events = buildEvents();
        final List<Map> parsedEvents = druidSink.parseEvents(events);
        assertThat(parsedEvents).isNotNull();
        assertThat(parsedEvents.size()).isEqualTo(2);

    }

    @Test
    public void parseEmptyEvents() throws Exception {
        druidSink = new DruidSink();
        List<Event> events = new ArrayList<Event>();
        final List<Map> parsedEvents = druidSink.parseEvents(events);
        assertThat(parsedEvents).isNotNull();
        assertThat(parsedEvents.size()).isEqualTo(0);

    }

    @Test(expected = DruidSinkException.class)
    public void parseInvalidEvents() {
        druidSink = new DruidSink();
        druidSink.parseEvents(null);

    }

    @Test
    public void processValidEvents() throws EventDeliveryException {
        setup();
        Transaction tx = channel.getTransaction();
        tx.begin();

        channel.put(getEvent());
        channel.put(getEvent());
        channel.put(getEvent());
        channel.put(getEvent());
        channel.put(getEvent());
        channel.put(getEvent());

        tx.commit();
        tx.close();

        druidSink.process();
    }

    private Event getEvent() {
        ObjectNode jsonBody = new ObjectNode(JsonNodeFactory.instance);
        jsonBody.put("field1", "foo");
        jsonBody.put("field2", 32);
        jsonBody.put("timestamp", new Date().getTime());

        Map<String, String> headers = new HashMap<String, String>();
        headers.put("field3", "bar"); // Overwrites the value defined in JSON body
        headers.put("field4", "64");
        headers.put("field5", "true");
        headers.put("field6", "1.0");
        headers.put("field7", "11");


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

    private List<Event> buildEvents() {
        List<Event> events = new ArrayList<Event>();
        events.add(createEvent("0"));
        events.add(createEvent("1"));

        return events;
    }

    private Event createEvent(String index) {
        ObjectNode jsonBody = new ObjectNode(JsonNodeFactory.instance);
        jsonBody.put("field1" + index, "foo");
        jsonBody.put("field2" + index, 32);

        Map<String, String> headers = new HashMap<String, String>();
        headers.put("header1" + index, "bar");
        headers.put("header2" + index, "64");
        headers.put("header3" + index, "true");
        headers.put("header4" + index, "1.0");
        headers.put("header5" + index, null);

        return EventBuilder.withBody(jsonBody.toString().getBytes(Charsets.UTF_8), headers);
    }
}