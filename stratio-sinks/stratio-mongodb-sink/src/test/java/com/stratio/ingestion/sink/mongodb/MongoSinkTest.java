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

import static org.fest.assertions.Assertions.assertThat;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fakemongo.Fongo;
import com.google.common.base.Charsets;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

@RunWith(JUnit4.class)
public class MongoSinkTest {

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
        mongoContext.put("batchSize", "1");
        mongoContext.put("mappingFile", "/mapping_definition_1.json");
        mongoContext.put("mongoUri", "INJECTED");

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

        ObjectNode jsonBody = new ObjectNode(JsonNodeFactory.instance);
        jsonBody.put("myString", "foo");
        jsonBody.put("myInt32", 32);

        Map<String, String> headers = new HashMap<String, String>();
        headers.put("myString", "bar"); // Overwrites the value defined in JSON body
        headers.put("myInt64", "64");
        headers.put("myBoolean", "true");
        headers.put("myDouble", "1.0");
        headers.put("myNull", null);

        Date myDate = new Date();
        headers.put("myDate", Long.toString(myDate.getTime()));

        headers.put("myString2", "baz");

        Event event = EventBuilder.withBody(jsonBody.toString().getBytes(Charsets.UTF_8), headers);
        channel.put(event);

        tx.commit();
        tx.close();

        mongoSink.process();

        DBObject result = fongo.getDB("test").getCollection("test").findOne();

        System.out.println(result.toString());

        assertThat(result).isNotNull();
        assertThat(result.get("myString")).isEqualTo("bar");
        assertThat(result.get("myInt32")).isEqualTo(32);
        assertThat(result.get("myInt32")).isInstanceOf(Integer.class);
        assertThat(result.get("myInt64")).isEqualTo(64);
        assertThat(result.get("myInt64")).isInstanceOf(Integer.class);
        assertThat(result.get("myBoolean")).isEqualTo(true);
        assertThat(result.get("myDouble")).isEqualTo(1.0);
        assertThat(result.get("myNull")).isNull();
        assertThat(result.get("myDate")).isEqualTo(myDate);
        assertThat(result.get("myString2")).isNull();
        assertThat(result.get("myStringMapped")).isEqualTo("baz");
    }

    @Test
    public void emptyChannel() throws Exception {
        mongoSink.process();
        assertThat(fongo.getDB("test").getCollection("test").count()).isEqualTo(0);
    }

    @Test
    public void batchSize() throws Exception {
        setField(mongoSink, "batchSize", 100);
        for (int i = 0; i < 200; i++) {
            Transaction tx = channel.getTransaction();
            tx.begin();
            Event event = EventBuilder.withBody("{ }".getBytes(Charsets.UTF_8));
            channel.put(event);
            tx.commit();
            tx.close();
        }
        mongoSink.process();
        assertThat(fongo.getDB("test").getCollection("test").count()).isEqualTo(100);
    }

    @Test
    public void noFullBatch() throws Exception {
        setField(mongoSink, "batchSize", 5);
        for (int i = 0; i < 3; i++) {
            Transaction tx = channel.getTransaction();
            tx.begin();
            Event event = EventBuilder.withBody("{ }".getBytes(Charsets.UTF_8));
            channel.put(event);
            tx.commit();
            tx.close();
        }
        mongoSink.process();
        assertThat(fongo.getDB("test").getCollection("test").count()).isEqualTo(3);
    }

    @Test(expected = MongoSinkException.class)
    public void errorOnProcess() throws Exception {
        DBCollection mockedCollection = Mockito.mock(DBCollection.class);
        Mockito.when(mockedCollection.save(Mockito.any(DBObject.class))).thenThrow(Error.class);
        setField(mongoSink, "mongoDefaultCollection", mockedCollection);
        Transaction tx = channel.getTransaction();
        tx.begin();
        Event event = EventBuilder.withBody("{ }".getBytes(Charsets.UTF_8));
        channel.put(event);
        tx.commit();
        tx.close();
        mongoSink.process();
    }

    @Test(expected = MongoSinkException.class)
    public void confSingleModeWithNoDefaultDB() throws Exception {
        final MongoSink mongoSink = new MongoSink();
        final Context context = new Context();
        context.put("dynamic", "false");
        context.put("mongoUri", "mongodb://localhost:10000");
        Configurables.configure(mongoSink, context);
    }

    @Test(expected = MongoSinkException.class)
    public void confSingleModeWithNoDefaultDBCollection() throws Exception {
        final MongoSink mongoSink = new MongoSink();
        final Context context = new Context();
        context.put("dynamic", "false");
        context.put("mongoUri", "mongodb://localhost:10000/test");
        Configurables.configure(mongoSink, context);
    }

}
