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
package com.stratio.ingestion.deserializer.jsonxpath;

import com.google.common.collect.Lists;
import com.stratio.ingestion.serialization.tracker.TransientPositionTracker;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableFileInputStream;
import org.apache.flume.serialization.ResettableInputStream;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flume.agent.embedded.EmbeddedAgent;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class JsonPathDeserializerTest {

    private static final String JSON_FILE= "example1.json";
    
    private static final String _ORDER_ID= "00000000-0000-0000-C000-000000000046";
    private static final String _PAYMENT_METHOD= "credit card";
    
    
    private ResettableInputStream getTestInputStream() throws Exception {
        return getTestInputStream(JSON_FILE);
    }

    private ResettableInputStream getTestInputStream(final String path) throws Exception {
        //File f = new File(this.getClass().getClassLoader().getResource(path).toURI());
        //return new ResettableFileInputStream(f, new TransientPositionTracker("dummy"));
        return new ResettableFileInputStream(new File("src/test/resources/" + path), new TransientPositionTracker("dummy"));
    }

    @Test
    public void testReadsAndMark() throws Exception {
        Context context = new Context();
        //context.put("expression", "$.store.book[*]");
        context.put("expression", "$.order_id");
        context.put("outputBody", "false");
        context.put("outputHeader", "order_id");
        EventDeserializer des = new JsonPathDeserializer.Builder().build(context, getTestInputStream());
        
        Event evt= des.readEvent();
        assertTrue(new String(evt.getBody()).contains(_ORDER_ID));
        assertEquals(_ORDER_ID, evt.getHeaders().get("order_id"));
        assertEquals(_PAYMENT_METHOD, evt.getHeaders().get("payment_method"));
        System.out.println("Event : "+ evt.toString());
        //validateReadAndMark(des);
    }

    @Test    
    public void copyToHeaders() throws Exception {
        Context context = new Context();
        context.put("expression", "$.lines[*]");        
        
        EventDeserializer des = new JsonPathDeserializer.Builder().build(context, getTestInputStream());
        
        Event evt= des.readEvent();
        assertTrue(new String(evt.getBody()).contains(_ORDER_ID));
        assertEquals(_ORDER_ID, evt.getHeaders().get("order_id"));
        assertEquals(_PAYMENT_METHOD, evt.getHeaders().get("payment_method"));
    }
    
    @Test    
    public void readMultipleEventes() throws Exception {
        Context context = new Context();
        context.put("expression", "$.lines[*]");        
        
        EventDeserializer des = new JsonPathDeserializer.Builder().build(context, getTestInputStream());
        
        List<Event> events= des.readEvents(2);
        Event evt= events.get(0);
        assertEquals(2, events.size());
        assertTrue(new String(evt.getBody()).contains(_ORDER_ID));
        assertEquals(_ORDER_ID, evt.getHeaders().get("order_id"));
        assertEquals(_PAYMENT_METHOD, evt.getHeaders().get("payment_method"));
        System.out.println("Event : "+ evt.toString());
    }    
    
    @Test
    public void embeddedTest() throws Exception {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("sources", "spoolSource");
        properties.put("sinks", "logSink");
        properties.put("channels", "c1");
        
        properties.put("sources.spoolSource.type", "spoolDir");
        
        properties.put("channel.type", "memory");
        properties.put("channel.capacity", "200");
        
        properties.put("logSink.type", "avro");
        properties.put("sink2.type", "avro");
        properties.put("sink1.hostname", "collector1.apache.org");
        properties.put("sink1.port", "5564");
        properties.put("sink2.hostname", "collector2.apache.org");
        properties.put("sink2.port",  "5565");
        properties.put("processor.type", "load_balance");
        properties.put("source.interceptors", "i1");
        properties.put("source.interceptors.i1.type", "static");
        properties.put("source.interceptors.i1.key", "key1");
        properties.put("source.interceptors.i1.value", "value1");

        EmbeddedAgent agent = new EmbeddedAgent("myagent");

        agent.configure(properties);
        agent.start();

        List<Event> events = Lists.newArrayList();

        events.add(event);
        events.add(event);
        events.add(event);
        events.add(event);

        agent.putAll(events);

        

        agent.stop();
        
        
        
    }


}
