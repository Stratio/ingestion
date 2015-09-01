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
import java.util.List;

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
    @Ignore
    public void testReset() throws Exception {
        Context context = new Context();
        context.put("expression", "$.store.book[*].title");
        EventDeserializer des = new JsonPathDeserializer.Builder().build(context, getTestInputStream());
        validateReset(des);
    }

    @Test
    @Ignore
    public void testHeader() throws Exception {
        Context context = new Context();
        context.put("expression", "$.store.book[*]");
        context.put("outputHeader", "myHeader");
        context.put("outputBody", "false");
        EventDeserializer des = new JsonPathDeserializer.Builder().build(context, getTestInputStream());
        validateReadAndMarkWithHeader(des);
    }

    @Test(expected = RuntimeException.class)
    @Ignore
    public void testBadJSON() throws Exception {
        new JsonPathDeserializer.Builder().build(new Context(), getTestInputStream("bad.json"));
    }

    private void validateReset(EventDeserializer des) throws IOException {
        Event evt = des.readEvent();
        assertEquals("Sayings of the Century", new String(evt.getBody()));
        des.mark();

        List<Event> events = des.readEvents(3);
        assertEquals(3, events.size());
        assertEquals("Sword of Honour", new String(events.get(0).getBody()));
        assertEquals("Moby Dick", new String(events.get(1).getBody()));
        assertEquals("The Lord of the Rings", new String(events.get(2).getBody()));

        des.reset(); // reset!

        events = des.readEvents(3);
        assertEquals(3, events.size());
        assertEquals("Sword of Honour", new String(events.get(0).getBody()));
        assertEquals("Moby Dick", new String(events.get(1).getBody()));
        assertEquals("The Lord of the Rings", new String(events.get(2).getBody()));

        evt = des.readEvent();
        Assert.assertNull("Event should be null because there are no more books " + "left to read", evt);

    }

    private void validateHeaders(EventDeserializer des) throws IOException {
        List<Event> events = des.readEvents(4);
        Assert.assertTrue(events.size() == 4);

        for (Event evt : events) {
            Assert.assertEquals(evt.getHeaders().get("author"), "J K. Rowling");
        }
    }

    private void validateReadAndMark(EventDeserializer des) throws IOException {
        Event evt;

        evt = des.readEvent();
        assertTrue(new String(evt.getBody()).contains("Nigel Rees"));
        des.mark();

        evt = des.readEvent();
        assertTrue(new String(evt.getBody()).contains("Evelyn Waugh"));
        des.mark(); // reset!

        List<Event> readEvents = des.readEvents(2);
        assertEquals(2, readEvents.size());

        evt = des.readEvent();
        assertNull("Event should be null because there are no more books " + "left to read", evt);

        des.mark();
        des.mark();
        des.close();
    }

    private void validateReadAndMarkWithHeader(EventDeserializer des) throws IOException {
        Event evt;

        evt = des.readEvent();
        System.out.println(evt.getHeaders().get("myHeader"));
        assertTrue(evt.getHeaders().get("myHeader").contains("Nigel Rees"));
        des.mark();

        evt = des.readEvent();
        assertTrue(evt.getHeaders().get("myHeader").contains("Evelyn Waugh"));
        des.mark(); // reset!

        List<Event> readEvents = des.readEvents(2);
        assertEquals(2, readEvents.size());

        evt = des.readEvent();
        assertNull("Event should be null because there are no more books " + "left to read", evt);

        des.mark();
        des.mark();
        des.close();
    }


}
