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
package com.stratio.ingestion.sink.mongodb;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fakemongo.Fongo;
import com.google.common.base.Charsets;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.fest.assertions.Assertions.assertThat;

@RunWith(JUnit4.class)
public class MongoSinkDynamicTest {

    private Fongo fongo;
    private MongoSink mongoSink;
    private Channel channel;

    private void setField(Object obj, String fieldName, Object val) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(obj, val);
        field.setAccessible(false);
    }

    private void injectFongo(MongoSink mongoSink) {
        try {
            MongoClient mongoClient = fongo.getMongo();
            DB mongoDefaultDb = mongoClient.getDB("test");
            DBCollection mongoDefaultCollection = mongoDefaultDb.getCollection("test");
            setField(mongoSink, "mongoClient", mongoClient);
            setField(mongoSink, "mongoDefaultDb", mongoDefaultDb);
            setField(mongoSink, "mongoDefaultCollection", mongoDefaultCollection);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Before
    public void prepareMongo() throws Exception {
        fongo = new Fongo("mongo test server");

        Context mongoContext = new Context();
        mongoContext.put("batchSize", "3");
        mongoContext.put("mappingFile", "/mapping_definition_1.json");
        mongoContext.put("mongoUri", "INJECTED");
        mongoContext.put("dynamic", "true");

        mongoSink = new MongoSink();

        injectFongo(mongoSink);
        Configurables.configure(mongoSink, mongoContext);

        Context channelContext = new Context();
        channelContext.put("capacity", "10000");
        channelContext.put("transactionCapacity", "200");

        channel = new MemoryChannel();
        channel.setName("junitChannel");
        Configurables.configure(channel, channelContext);

        mongoSink.setChannel(channel);

        channel.start();
        mongoSink.start();
    }

    @After
    public void tearDownMongo() {
        channel.stop();
        mongoSink.stop();
    }

    @Test
    public void basicTest() throws Exception {
        Transaction tx = channel.getTransaction();
        tx.begin();

        Map<String, String> headers = new HashMap<String, String>();
        headers.put("myString", "test1");
        Event event = EventBuilder.withBody(new byte[0], headers);
        channel.put(event);

        headers.clear();
        headers.put("myString", "test2");
        headers.put("collection", "test2coll");
        event = EventBuilder.withBody(new byte[0], headers);
        channel.put(event);

        headers.clear();
        headers.put("myString", "test3");
        headers.put("db", "test3db");
        headers.put("collection", "test3coll");
        event = EventBuilder.withBody(new byte[0], headers);
        channel.put(event);

        tx.commit();
        tx.close();

        mongoSink.process();

        DBObject result = fongo.getDB("test").getCollection("test").findOne();
        assertThat(result.get("myString")).isEqualTo("test1");

        result = fongo.getDB("test").getCollection("test2coll").findOne();
        assertThat(result.get("myString")).isEqualTo("test2");

        result = fongo.getDB("test3db").getCollection("test3coll").findOne();
        assertThat(result.get("myString")).isEqualTo("test3");
    }

}
