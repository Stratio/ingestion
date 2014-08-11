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
package com.stratio.ingestion.sink.kafka;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;

@RunWith(JUnit4.class)
public class KafkaSinkTest {

    private static final String CLIENT_ID = "testClient";

    private ZookeeperServer zookeeperServer;
    private KafkaServer kafkaServer;
    private Channel channel;
    private KafkaSink kafkaSink;
    private SimpleConsumer simpleConsumer;

    @Before
    public void setUp() {

        try {
            zookeeperServer = new ZookeeperServer();
            zookeeperServer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            kafkaServer = new KafkaServer();
            kafkaServer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        simpleConsumer = new SimpleConsumer("localhost", 9092, 60000, 1024, CLIENT_ID);

        kafkaSink = new KafkaSink();

        Context kafkaContext = new Context();
        kafkaContext.put("topic", "test");
        kafkaContext.put("writeBody", "false");
        kafkaContext.put("kafka.metadata.broker.list", "localhost:9092");
        kafkaContext.put("kafka.serializer.class", "kafka.serializer.StringEncoder");

        Configurables.configure(kafkaSink, kafkaContext);

        Context channelContext = new Context();
        channelContext.put("capacity", "10000");
        channelContext.put("transactionCapacity", "200");

        channel = new MemoryChannel();
        channel.setName("junitChannel");
        Configurables.configure(channel, channelContext);

        kafkaSink.setChannel(channel);

        channel.start();
        kafkaSink.start();

    }

    @After
    public void tearDown() throws IOException {
        kafkaSink.stop();
        simpleConsumer.close();
        kafkaServer.shutdown();
        zookeeperServer.shutdown();
    }

    @Test
    public void test() throws EventDeliveryException, UnsupportedEncodingException {
        Transaction tx = channel.getTransaction();
        tx.begin();

        ObjectNode jsonBody = new ObjectNode(JsonNodeFactory.instance);
        jsonBody.put("myString", "foo");
        jsonBody.put("myInt32", 32);

        Map<String, String> headers = new HashMap<>();
        headers.put("myString", "bar");
        headers.put("myInt64", "64");
        headers.put("myBoolean", "true");
        headers.put("myDouble", "1.0");
        headers.put("myNull", "foobar");

        Event event = EventBuilder.withBody(jsonBody.toString().getBytes(Charsets.UTF_8), headers);
        channel.put(event);

        tx.commit();
        tx.close();

        kafkaSink.process();

        kafka.api.FetchRequest req = new FetchRequestBuilder().clientId(CLIENT_ID)
                .addFetch("test", 0, 0L, 100).build();
        FetchResponse fetchResponse = simpleConsumer.fetch(req);
        ByteBufferMessageSet messageSet = fetchResponse.messageSet("test", 0);

        Assert.assertTrue(messageSet.sizeInBytes() > 0);
        for (MessageAndOffset messageAndOffset : messageSet) {
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            String message = new String(bytes, "UTF-8");
            Assert.assertNotNull(message);
            Assert.assertEquals(message, "{\"myString\":\"foo\",\"myInt32\":32}");
        }
    }
}
