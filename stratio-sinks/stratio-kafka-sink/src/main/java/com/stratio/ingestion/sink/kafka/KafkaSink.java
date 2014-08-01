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

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

//@formatter:off
/**
*
* <p>Reads events from a channel and writes them to Kafka.</p>
*
* Configuration parameters are:
*
* <p>
* <ul>
* <li><tt>topic</tt>: Name of topic where event will be sent to. Defaults to <tt>test</tt>.</li>
* <li><tt>kafka.<kafka-producer-property></tt>: This sink accept any kafka producer property. Just write it after prefix <tt>kafka.</tt>.</li>
* </ul>
* </p>
*
*/
//@formatter:on
public class KafkaSink extends AbstractSink implements Configurable {

    private static final Logger log = LoggerFactory.getLogger(KafkaSink.class);
    private static final String CONF_TOPIC = "topic";
    private static final String CONF_KAFKA = "kafka.";
    
    private static final String DEFAULT_TOPIC = "test";

    private String topic;
    private Producer<String, String> producer;
    
    @Override
    public void configure(Context context) {
        topic = context.getString(CONF_TOPIC, DEFAULT_TOPIC);
        if (topic == null) {
            throw new ConfigurationException("Kafka topic must be specified.");
        }
        ImmutableMap<String, String> subProperties = context.getSubProperties(CONF_KAFKA);
        Properties properties = new Properties();
        properties.putAll(subProperties);
        
        producer = new Producer<String, String>(new ProducerConfig(properties));
    }

    @Override
    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        try {
            tx.begin();
            Event event = channel.take();
            if (event == null) {
                tx.commit();
                return Status.READY;

            }
            producer.send(new KeyedMessage<String, String>(topic, new String(event
                    .getBody())));
            tx.commit();
            return Status.READY;
        } catch (Exception e) {
            try {
                tx.rollback();
                return Status.BACKOFF;
            } catch (Exception e2) {
                log.error("Rollback Exception:{}", e2);
            }       
            log.error("KafkaSink Exception:{}", e);
            return Status.BACKOFF;
        } finally {
            tx.close();
        }
    }

    @Override
    public synchronized void start() {
        super.start();
    }

    @Override
    public synchronized void stop() {
        producer.close();
        super.stop();
    }

}
