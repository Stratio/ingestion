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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.avro.data.Json;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableFileInputStream;
import org.apache.flume.serialization.ResettableInputStream;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import com.jayway.jsonpath.*;

import com.jayway.jsonpath.internal.JsonReader;
import com.stratio.ingestion.serialization.tracker.TransientPositionTracker;

@RunWith(JUnit4.class)
public class JsonXpathDeserializerTest {
//
//    private JsonPath jsonPath;
//
//
//    private JsonReader json = new JsonReader();
//    private JsonReader jsonReader(String path){
////        File jsonFile = new File("src/test/resources/" + path);
//        json.add("src/test/resources/" + path, "lacteos.json");
//    }

//    private ResettableInputStream getTestInputStream() throws IOException {
//      return getTestInputStream("lacteos.json");
//    }

    private ResettableInputStream getTestInputStream(final String path) throws IOException {
      return new ResettableFileInputStream(new File("src/test/resources/" + path), new TransientPositionTracker("dummy"));
    }

//    @Test
//    public void testBooks() throws IOException {
//        String jsonPath = "$.store.book[1].author";
//
//        File jsonFile = new File("src/test/resources/books.json");
//
//        System.out.println("Author: "+JsonPath.read(jsonFile, jsonPath));
//
//    }

//    @Test(expected = RuntimeException.class)
//    public void testLacteos() throws IOException {
//        File jsonFile = new File("src/test/resources/lacteos.json");
//
//        new JsonXpathDeserializer.Reader().jsonReader(jsonFile);
//    }

    @Test(expected = FileNotFoundException.class)
    public void testNoExists() throws IOException {
        Context context = new Context();
        context.put("expression", "/bookstore/book");
        EventDeserializer des = new JsonXpathDeserializer.Builder().build(context, getTestInputStream(""));
        //        List<Event> events = des.readEvents(4);
        //        assertEquals(4, events.size());
        //        for (final Event event : events) {
        //            assertNotNull(event);
        //        }
    }

    @Test(expected = RuntimeException.class)
    public void testEmpty() throws IOException {
        Context context = new Context();
        context.put("expression", "/bookstore/book");
        EventDeserializer des = new JsonXpathDeserializer.Builder().build(context, getTestInputStream("vacio.json"));
        //        List<Event> events = des.readEvents(4);
        //        assertEquals(4, events.size());
        //        for (final Event event : events) {
        //            assertNotNull(event);
        //        }
    }

    @Test()
    public void testLacteos() throws IOException {
        Context context = new Context();
        context.put("expression", "/bookstore/book");
        EventDeserializer des = new JsonXpathDeserializer.Builder().build(context, getTestInputStream("lacteos.json"));
//        List<Event> events = des.readEvents(4);
//        System.out.println(events.size());
//        assertEquals(4, events.size());
//        for (final Event event : events) {
//            assertNotNull(event);
//        }
    }

    @Test(expected = RuntimeException.class)
    public void testNoProducts() throws IOException {
        Context context = new Context();
        context.put("expression", "/bookstore/book");
        EventDeserializer des = new JsonXpathDeserializer.Builder().build(context, getTestInputStream("noLacteos"
                + ".json"));
        //        List<Event> events = des.readEvents(4);
        //        System.out.println(events.size());
        //        assertEquals(4, events.size());
        //        for (final Event event : events) {
        //            assertNotNull(event);
        //        }
    }

    @Test
    public void testHeader() throws IOException {
        Context context = new Context();
        context.put("expression", "timestamp");
        context.put("outputHeader", "myHeader");
        context.put("outputBody", "false");
        EventDeserializer des = new JsonXpathDeserializer.Builder().build(context, getTestInputStream("lacteos.json"));
        validateReadAndMarkWithHeader(des);
    }

    private void validateReadAndMarkWithHeader(EventDeserializer des) throws IOException {
        Event evt;

        evt = des.readEvent();
//        System.out.println(evt.getHeaders().get("myHeader"));
        assertTrue(evt.getHeaders().get("myHeader").contains("YOGURT"));
        des.mark();

        evt = des.readEvent();
        assertTrue(evt.getHeaders().get("myHeader").contains("MILK"));
        des.mark();

        evt = des.readEvent();
        assertTrue(evt.getHeaders().get("myHeader").contains("WATER"));
        des.mark(); // reset!
        des.mark();
        des.reset();
        List<Event> readEvents = des.readEvents(3);
        assertEquals(3, readEvents.size());

        evt = des.readEvent();
        assertNull("Event should be null because there are no more books " + "left to read", evt);

        des.mark();
        des.mark();
        des.close();
    }


}
