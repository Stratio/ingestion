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

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

@RunWith(JUnit4.class)
public class MongoSinkUpdateInsteadReplaceTest {

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
        mongoContext.put("mappingFile", "/mapping_definition_update.json");
        mongoContext.put("mongoUri", "INJECTED");
        mongoContext.put("dynamic", "true");
        mongoContext.put("updateInsteadReplace", "true");

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
    	
    	fongo.getDB("test").getCollection("test").insert(
    			new BasicDBObject().
    			append("_id", "documentPrimaryKey").
    			append("fieldToKeep1", "initialvalue1").
    			append("fieldToKeep2", "initialvalue2").
    			append("fieldToUpdate", "toupdate")
    			);
    	
        Transaction tx = channel.getTransaction();
        tx.begin();

        Map<String, String> headers = new HashMap<String, String>();
        headers.put("_id", "documentPrimaryKey");
        headers.put("fieldToUpdate", "updated");
        headers.put("newField", "added");
        Event event = EventBuilder.withBody(new byte[0], headers);
        channel.put(event);

        tx.commit();
        tx.close();

        mongoSink.process();

        DBObject result = fongo.getDB("test").getCollection("test").findOne();
        assertThat(result.get("_id")).isEqualTo("documentPrimaryKey");
        assertThat(result.get("fieldToKeep1")).isEqualTo("initialvalue1");
        assertThat(result.get("fieldToKeep2")).isEqualTo("initialvalue2");
        assertThat(result.get("fieldToUpdate")).isEqualTo("updated");
        assertThat(result.get("newField")).isEqualTo("added");
    }

}
