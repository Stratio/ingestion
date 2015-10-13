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

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Sink.Status;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fakemongo.Fongo;
import com.google.common.base.Charsets;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

@RunWith(JUnit4.class)
public class MongoSinkSaveOperationsTest {

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
    public void setTest() throws Exception {
    	{
	        Transaction tx = channel.getTransaction();
	        tx.begin();
	
	        ObjectNode jsonBody = new ObjectNode(JsonNodeFactory.instance);
	        jsonBody.put("doc_id", "123456");
	        jsonBody.put("field_1", "value_1");
	        jsonBody.put("field_2", "value_2");
	        jsonBody.put("field_3", "value_3");
	
	        Event event = EventBuilder.withBody(jsonBody.toString().getBytes(Charsets.UTF_8));
	        channel.put(event);
	
	        tx.commit();
	        tx.close();
	
	        mongoSink.process();
	
	        DBObject result = fongo.getDB("test").getCollection("test").findOne();
	
	        assertThat(result).isNotNull();
	        assertThat(result.get("doc_id")).isEqualTo("123456");
	        assertThat(result.get("field_1")).isEqualTo("value_1");
	        assertThat(result.get("field_2")).isEqualTo("value_2");
	        assertThat(result.get("field_3")).isEqualTo("value_3");
    	}

        setField(mongoSink, "saveOperation", com.stratio.ingestion.sink.mongodb.MongoSink.SAVE_OPERATION.SET);
        setField(mongoSink, "idFieldName", "doc_id");
        
    	{
	        Transaction tx = channel.getTransaction();
	        tx.begin();
	
	        ObjectNode jsonBody = new ObjectNode(JsonNodeFactory.instance);
	        jsonBody.put("doc_id", "123456");
	        jsonBody.put("field_1", "value_1_2");
	        jsonBody.put("field_2", "value_2_2");
	
	        Event event = EventBuilder.withBody(jsonBody.toString().getBytes(Charsets.UTF_8));
	        channel.put(event);
	
	        tx.commit();
	        tx.close();
	
	        mongoSink.process();
	
	        DBObject result = fongo.getDB("test").getCollection("test").findOne();
	
	        assertThat(result).isNotNull();
	        assertThat(result.get("doc_id")).isEqualTo("123456");
	        assertThat(result.get("field_1")).isEqualTo("value_1_2");
	        assertThat(result.get("field_2")).isEqualTo("value_2_2");
	        assertThat(result.get("field_3")).isEqualTo("value_3");
    	}
    }

    @Test
    public void addToSetTest() throws Exception {
    	{
	        Transaction tx = channel.getTransaction();
	        tx.begin();
	
	        ObjectNode jsonBody = new ObjectNode(JsonNodeFactory.instance);
	        jsonBody.put("doc_id", "123456");
	        
	    	ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
	    	array.add(1);
	        jsonBody.set("array", array);
	
	        Event event = EventBuilder.withBody(jsonBody.toString().getBytes(Charsets.UTF_8));
	        channel.put(event);
	
	        tx.commit();
	        tx.close();
	
	        mongoSink.process();
	
	        DBObject result = fongo.getDB("test").getCollection("test").findOne();
	
	        assertThat(result).isNotNull();
	        assertThat(result.get("doc_id")).isEqualTo("123456");
	        assertThat(result.get("array")).isNotNull();
	        assertThat(result.get("array")).isInstanceOf(com.mongodb.BasicDBList.class);
	        assertThat(((com.mongodb.BasicDBList)result.get("array")).size()).isEqualTo(1);
	        assertThat(((com.mongodb.BasicDBList)result.get("array")).get(0)).isEqualTo(1);
    	}

        setField(mongoSink, "saveOperation", com.stratio.ingestion.sink.mongodb.MongoSink.SAVE_OPERATION.ADD_TO_SET);
        setField(mongoSink, "idFieldName", "doc_id");
        setField(mongoSink, "fieldName", "array");
        
    	{
	        Transaction tx = channel.getTransaction();
	        tx.begin();
	
	        ObjectNode jsonBody = new ObjectNode(JsonNodeFactory.instance);
	        jsonBody.put("doc_id", "123456");
	        
	    	ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
	    	array.add(2);
	        jsonBody.set("array", array);
	
	        Event event = EventBuilder.withBody(jsonBody.toString().getBytes(Charsets.UTF_8));
	        channel.put(event);
	
	        tx.commit();
	        tx.close();
	
	        mongoSink.process();
	
	        DBObject result = fongo.getDB("test").getCollection("test").findOne();
	
	        assertThat(result).isNotNull();
	        assertThat(result.get("doc_id")).isEqualTo("123456");
	        assertThat(result.get("array")).isNotNull();
	        assertThat(result.get("array")).isInstanceOf(com.mongodb.BasicDBList.class);
	        assertThat(((com.mongodb.BasicDBList)result.get("array")).size()).isEqualTo(2);
	        assertThat(((com.mongodb.BasicDBList)result.get("array")).get(0)).isEqualTo(1);
	        assertThat(((com.mongodb.BasicDBList)result.get("array")).get(1)).isEqualTo(2);
    	}
        
    	{
	        Transaction tx = channel.getTransaction();
	        tx.begin();
	
	        ObjectNode jsonBody = new ObjectNode(JsonNodeFactory.instance);
	        jsonBody.put("doc_id", "123456");
	        
	        jsonBody.put("array", 3);
	
	        Event event = EventBuilder.withBody(jsonBody.toString().getBytes(Charsets.UTF_8));
	        channel.put(event);
	
	        tx.commit();
	        tx.close();
	
	        mongoSink.process();
	
	        DBObject result = fongo.getDB("test").getCollection("test").findOne();
	
	        assertThat(result).isNotNull();
	        assertThat(result.get("doc_id")).isEqualTo("123456");
	        assertThat(result.get("array")).isNotNull();
	        assertThat(result.get("array")).isInstanceOf(com.mongodb.BasicDBList.class);
	        assertThat(((com.mongodb.BasicDBList)result.get("array")).size()).isEqualTo(3);
	        assertThat(((com.mongodb.BasicDBList)result.get("array")).get(0)).isEqualTo(1);
	        assertThat(((com.mongodb.BasicDBList)result.get("array")).get(1)).isEqualTo(2);
	        assertThat(((com.mongodb.BasicDBList)result.get("array")).get(2)).isEqualTo(3);
    	}
    }

    @Test
    public void addToSetArrayTest() throws Exception {
    	{
	        Transaction tx = channel.getTransaction();
	        tx.begin();
	
	        ObjectNode jsonBody = new ObjectNode(JsonNodeFactory.instance);
	        jsonBody.put("doc_id", "123456");
	        
	    	ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
	    	
	    	ObjectNode arrayObject = new ObjectNode(JsonNodeFactory.instance);
	    	arrayObject.put("field", "value1");	    	
	    	array.add(arrayObject);	    	
	    	
	        jsonBody.set("array", array);
	
	        Event event = EventBuilder.withBody(jsonBody.toString().getBytes(Charsets.UTF_8));
	        channel.put(event);
	
	        tx.commit();
	        tx.close();
	
	        mongoSink.process();
	
	        DBObject result = fongo.getDB("test").getCollection("test").findOne();
	
	        assertThat(result).isNotNull();
	        assertThat(result.get("doc_id")).isEqualTo("123456");
	        assertThat(result.get("array")).isNotNull();
	        assertThat(result.get("array")).isInstanceOf(com.mongodb.BasicDBList.class);
	        assertThat(((com.mongodb.BasicDBList)result.get("array")).size()).isEqualTo(1);
	        assertThat(((com.mongodb.BasicDBObject)((com.mongodb.BasicDBList)result.get("array")).get(0)).get("field")).isEqualTo("value1");
    	}

        setField(mongoSink, "saveOperation", com.stratio.ingestion.sink.mongodb.MongoSink.SAVE_OPERATION.ADD_TO_SET);
        setField(mongoSink, "idFieldName", "doc_id");
        setField(mongoSink, "fieldName", "array");
        
    	{
	        Transaction tx = channel.getTransaction();
	        tx.begin();
	
	        ObjectNode jsonBody = new ObjectNode(JsonNodeFactory.instance);
	        jsonBody.put("doc_id", "123456");
	        
	    	ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
	    	ObjectNode arrayObject = new ObjectNode(JsonNodeFactory.instance);
	    	arrayObject.put("field", "value2");	    	
	    	array.add(arrayObject);
	        jsonBody.set("array", array);
	
	        Event event = EventBuilder.withBody(jsonBody.toString().getBytes(Charsets.UTF_8));
	        channel.put(event);
	
	        tx.commit();
	        tx.close();
	
	        mongoSink.process();
	
	        DBObject result = fongo.getDB("test").getCollection("test").findOne();
	
	        assertThat(result).isNotNull();
	        assertThat(result.get("doc_id")).isEqualTo("123456");
	        assertThat(result.get("array")).isNotNull();
	        assertThat(result.get("array")).isInstanceOf(com.mongodb.BasicDBList.class);
	        assertThat(((com.mongodb.BasicDBList)result.get("array")).size()).isEqualTo(2);
	        assertThat(((com.mongodb.BasicDBObject)((com.mongodb.BasicDBList)result.get("array")).get(0)).get("field")).isEqualTo("value1");
	        assertThat(((com.mongodb.BasicDBObject)((com.mongodb.BasicDBList)result.get("array")).get(1)).get("field")).isEqualTo("value2");
    	}
        
    	{
	        Transaction tx = channel.getTransaction();
	        tx.begin();
	
	        ObjectNode jsonBody = new ObjectNode(JsonNodeFactory.instance);
	        jsonBody.put("doc_id", "123456");
	        
	        ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
	    	ObjectNode arrayObject = new ObjectNode(JsonNodeFactory.instance);
	    	arrayObject.put("field", "value3");	    	
	    	array.add(arrayObject);
	        jsonBody.set("array", array);
	
	        Event event = EventBuilder.withBody(jsonBody.toString().getBytes(Charsets.UTF_8));
	        channel.put(event);
	
	        tx.commit();
	        tx.close();
	
	        mongoSink.process();
	
	        DBObject result = fongo.getDB("test").getCollection("test").findOne();
	
	        assertThat(result).isNotNull();
	        assertThat(result.get("doc_id")).isEqualTo("123456");
	        assertThat(result.get("array")).isNotNull();
	        assertThat(result.get("array")).isInstanceOf(com.mongodb.BasicDBList.class);
	        assertThat(((com.mongodb.BasicDBList)result.get("array")).size()).isEqualTo(3);
	        assertThat(((com.mongodb.BasicDBObject)((com.mongodb.BasicDBList)result.get("array")).get(0)).get("field")).isEqualTo("value1");
	        assertThat(((com.mongodb.BasicDBObject)((com.mongodb.BasicDBList)result.get("array")).get(1)).get("field")).isEqualTo("value2");
	        assertThat(((com.mongodb.BasicDBObject)((com.mongodb.BasicDBList)result.get("array")).get(2)).get("field")).isEqualTo("value3");
    	}
    }

    @Test
    public void updateNoUpsertTest() throws Exception {
        setField(mongoSink, "saveOperation", com.stratio.ingestion.sink.mongodb.MongoSink.SAVE_OPERATION.UPDATE);
        setField(mongoSink, "idFieldName", "docdoc_id");
        
    	{
	        Transaction tx = channel.getTransaction();
	        tx.begin();
	
	        ObjectNode jsonBody = new ObjectNode(JsonNodeFactory.instance);
	        jsonBody.put("docdoc_id", "123456");
	        jsonBody.put("field", "VALUE");
	
	        Event event = EventBuilder.withBody(jsonBody.toString().getBytes(Charsets.UTF_8));
	        channel.put(event);
	
	        tx.commit();
	        tx.close();
	
	        mongoSink.process();
	
	        DBObject result = fongo.getDB("test").getCollection("test").findOne();
	
	        assertThat(result).isNull();
    	}
    }

    @Test
    public void updateUpsertTest() throws Exception {
        setField(mongoSink, "saveOperation", com.stratio.ingestion.sink.mongodb.MongoSink.SAVE_OPERATION.UPDATE);
        setField(mongoSink, "idFieldName", "docdoc_id");
        setField(mongoSink, "upsertUpdate", Boolean.TRUE);
        
    	{
	        Transaction tx = channel.getTransaction();
	        tx.begin();
	
	        ObjectNode jsonBody = new ObjectNode(JsonNodeFactory.instance);
	        jsonBody.put("docdoc_id", "123456");
	        jsonBody.put("field", "VALUE");
	
	        Event event = EventBuilder.withBody(jsonBody.toString().getBytes(Charsets.UTF_8));
	        channel.put(event);
	
	        tx.commit();
	        tx.close();
	
	        mongoSink.process();
	
	        DBObject result = fongo.getDB("test").getCollection("test").findOne();

	        assertThat(result).isNotNull();
	        assertThat(result.get("docdoc_id")).isEqualTo("123456");
	        assertThat(result.get("field")).isEqualTo("VALUE");
    	}
    }

    @Test
    public void updateTest() throws Exception {
    	{
	        Transaction tx = channel.getTransaction();
	        tx.begin();
	
	        ObjectNode jsonBody = new ObjectNode(JsonNodeFactory.instance);
	        jsonBody.put("docdoc_id", "123456");
	        jsonBody.put("field", "VALUE");
	
	        Event event = EventBuilder.withBody(jsonBody.toString().getBytes(Charsets.UTF_8));
	        channel.put(event);
	
	        tx.commit();
	        tx.close();
	
	        mongoSink.process();
	
	        DBObject result = fongo.getDB("test").getCollection("test").findOne();
		
	        assertThat(result).isNotNull();
	        assertThat(result.get("docdoc_id")).isEqualTo("123456");
	        assertThat(result.get("field")).isEqualTo("VALUE");
    	}

        setField(mongoSink, "saveOperation", com.stratio.ingestion.sink.mongodb.MongoSink.SAVE_OPERATION.UPDATE);
        setField(mongoSink, "idFieldName", "docdoc_id");
        
    	{
	        Transaction tx = channel.getTransaction();
	        tx.begin();
	
	        ObjectNode jsonBody = new ObjectNode(JsonNodeFactory.instance);
	        jsonBody.put("docdoc_id", "123456");
	        jsonBody.put("field", "VALUE_2");
	
	        Event event = EventBuilder.withBody(jsonBody.toString().getBytes(Charsets.UTF_8));
	        channel.put(event);
	
	        tx.commit();
	        tx.close();
	
	        mongoSink.process();
	
	        DBObject result = fongo.getDB("test").getCollection("test").findOne();
	
	        assertThat(result).isNotNull();
	        assertThat(result.get("docdoc_id")).isEqualTo("123456");
	        assertThat(result.get("field")).isEqualTo("VALUE_2");
    	}
    }

    public void errorIdFieldNameNoFound() throws Exception {
        setField(mongoSink, "saveOperation", com.stratio.ingestion.sink.mongodb.MongoSink.SAVE_OPERATION.ADD_TO_SET);
        setField(mongoSink, "idFieldName", "doc_id");
        setField(mongoSink, "fieldName", "array");
        
    	{
	        Transaction tx = channel.getTransaction();
	        tx.begin();
	
	        ObjectNode jsonBody = new ObjectNode(JsonNodeFactory.instance);
	        
	    	ArrayNode array = new ArrayNode(JsonNodeFactory.instance);
	    	array.add(2);
	        jsonBody.set("array", array);
	
	        Event event = EventBuilder.withBody(jsonBody.toString().getBytes(Charsets.UTF_8));
	        channel.put(event);
	
	        tx.commit();
	        tx.close();
	
	        Status status = mongoSink.process();
	
	        assertThat(status).isEqualTo(Status.BACKOFF);
    	}
    }

    public void errorFieldNameNoFound() throws Exception {
        setField(mongoSink, "saveOperation", com.stratio.ingestion.sink.mongodb.MongoSink.SAVE_OPERATION.ADD_TO_SET);
        setField(mongoSink, "idFieldName", "doc_id");
        setField(mongoSink, "fieldName", "array");
        
    	{
	        Transaction tx = channel.getTransaction();
	        tx.begin();
	
	        ObjectNode jsonBody = new ObjectNode(JsonNodeFactory.instance);
	        jsonBody.put("doc_id", "123456");
	
	        Event event = EventBuilder.withBody(jsonBody.toString().getBytes(Charsets.UTF_8));
	        channel.put(event);
	
	        tx.commit();
	        tx.close();
	
	        Status status = mongoSink.process();
	
	        assertThat(status).isEqualTo(Status.BACKOFF);
    	}
    }

    @Test(expected = IllegalArgumentException.class)
    public void errorIdFieldNameNoSet() throws Exception {
        setField(mongoSink, "saveOperation", "SET");
        
        Transaction tx = channel.getTransaction();
        tx.begin();
        Event event = EventBuilder.withBody("{ }".getBytes(Charsets.UTF_8));
        channel.put(event);
        tx.commit();
        tx.close();
        mongoSink.process();
    }

    @Test(expected = IllegalArgumentException.class)
    public void errorfieldNameNoSet() throws Exception {
        setField(mongoSink, "saveOperation", "ADD_TO_SET");
        setField(mongoSink, "idFieldName", "idFieldName");
        
        Transaction tx = channel.getTransaction();
        tx.begin();
        Event event = EventBuilder.withBody("{ }".getBytes(Charsets.UTF_8));
        channel.put(event);
        tx.commit();
        tx.close();
        mongoSink.process();
    }
}
