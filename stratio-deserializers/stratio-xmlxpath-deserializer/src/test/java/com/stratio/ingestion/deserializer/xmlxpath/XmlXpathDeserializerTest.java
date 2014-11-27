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
package com.stratio.ingestion.deserializer.xmlxpath;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.PositionTracker;
import org.apache.flume.serialization.SeekableFileInputStream;
import org.apache.flume.serialization.TransientPositionTracker;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.util.List;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class XmlXpathDeserializerTest {

    private static final Logger log = LoggerFactory.getLogger(XmlXpathDeserializerTest.class);

    private InputStream getTestInputStream() throws IOException {
      return getTestInputStream("test.xml");
    }

    private InputStream getTestInputStream(final String path) throws IOException {
      return new SeekableFileInputStream("src/test/resources/" + path);
    }

    @Test
    public void testReadsAndMark() throws IOException {
        Context context = new Context();
        context.put("expression", "/bookstore/book");
        EventDeserializer des = new XmlXpathDeserializer.Builder().build(context, getTestInputStream(), new TransientPositionTracker("target"));
        validateReadAndMark(des);
    }

    @Test
    public void testReset() throws IOException {
        Context context = new Context();
        context.put("expression", "/bookstore/book/title/text()");
        EventDeserializer des = new XmlXpathDeserializer.Builder().build(context, getTestInputStream(), new TransientPositionTracker("target"));
        validateReset(des);
    }

    @Test
    public void testHardReset() throws IOException {
        Context context = new Context();
        context.put("expression", "/bookstore/book/title/text()");
        PositionTracker positionTracker = new TransientPositionTracker("target");
        EventDeserializer des = new XmlXpathDeserializer.Builder().build(context, getTestInputStream(), positionTracker);
        Event evt = des.readEvent();
        assertEquals("Everyday Italian", new String(evt.getBody()));
        des.mark();
        des = new XmlXpathDeserializer.Builder().build(context, getTestInputStream(), positionTracker);

        List<Event> events = des.readEvents(3);
        assertEquals(3, events.size());
        assertEquals("Harry Potter", new String(events.get(0).getBody()));
        assertEquals("XQuery Kick Start", new String(events.get(1).getBody()));
        assertEquals("Learning XML", new String(events.get(2).getBody()));

        des.reset(); // reset!

        events = des.readEvents(3);
        assertEquals(3, events.size());
        assertEquals("Harry Potter", new String(events.get(0).getBody()));
        assertEquals("XQuery Kick Start", new String(events.get(1).getBody()));
        assertEquals("Learning XML", new String(events.get(2).getBody()));

        evt = des.readEvent();
        Assert.assertNull("Event should be null because there are no more books " + "left to read", evt);
    }

    @Test
    public void testHeader() throws IOException {
      Context context = new Context();
      context.put("expression", "/bookstore/book");
      context.put("outputHeader", "myHeader");
      context.put("outputBody", "false");
      EventDeserializer des = new XmlXpathDeserializer.Builder().build(context, getTestInputStream(), new TransientPositionTracker("target"));
      validateReadAndMarkWithHeader(des);
    }

    @Test
    public void testDocument2String() throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = factory.newDocumentBuilder();
        InputSource is = new InputSource(getTestInputStream());
        Document doc = docBuilder.parse(is);

        Context context = new Context();
        context.put("expression", "/bookstore/book/title");
        XmlXpathDeserializer des = new XmlXpathDeserializer(context, getTestInputStream(), new TransientPositionTracker("target"));

        Assert.assertNotNull(des.documentToString(doc));
        des.close();
    }

    @Test(expected = RuntimeException.class)
    public void testBadXML() throws IOException {
      EventDeserializer des = new XmlXpathDeserializer.Builder().build(new Context(), getTestInputStream("bad.xml"), new TransientPositionTracker("target"));
    }

    @Test()
    public void testXPathWithNS() throws IOException {
      Context context = new Context();
      context.put("expression", "/bookstore/book");
      EventDeserializer des = new XmlXpathDeserializer.Builder().build(context, getTestInputStream("ns.xml"), new TransientPositionTracker("target"));
      List<Event> events = des.readEvents(4);
      assertEquals(4, events.size());
      for (final Event event : events) {
        assertNotNull(event);
      }
    }

    @Test(expected = RuntimeException.class)
    public void testBadXPath() throws IOException {
      Context context = new Context();
      context.put("expression", "ñ/b\ngnklñ13");
      EventDeserializer des = new XmlXpathDeserializer.Builder().build(context, getTestInputStream(), new TransientPositionTracker("target"));
    }

    @Test(expected = RuntimeException.class)
    public void testNoOutputNoHeader() throws IOException {
      Context context = new Context();
      context.put("outputBody", "false");
      EventDeserializer des = new XmlXpathDeserializer.Builder().build(context, getTestInputStream(), new TransientPositionTracker("target"));
    }

    private void validateReadAndMark(EventDeserializer des) throws IOException {
        Event evt;

        evt = des.readEvent();
        assertTrue(new String(evt.getBody()).contains("Giada De Laurentiis"));
        des.mark();

        evt = des.readEvent();
        assertTrue(new String(evt.getBody()).contains("J K. Rowling"));
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
        assertTrue(evt.getHeaders().get("myHeader").contains("Giada De Laurentiis"));
        des.mark();

        evt = des.readEvent();
        assertTrue(evt.getHeaders().get("myHeader").contains("J K. Rowling"));
        des.mark(); // reset!

        List<Event> readEvents = des.readEvents(2);
        assertEquals(2, readEvents.size());

        evt = des.readEvent();
        assertNull("Event should be null because there are no more books " + "left to read", evt);

        des.mark();
        des.mark();
        des.close();
    }

    private void validateReset(EventDeserializer des) throws IOException {
        Event evt = des.readEvent();
        assertEquals("Everyday Italian", new String(evt.getBody()));
        des.mark();

        List<Event> events = des.readEvents(3);
        assertEquals(3, events.size());
        assertEquals("Harry Potter", new String(events.get(0).getBody()));
        assertEquals("XQuery Kick Start", new String(events.get(1).getBody()));
        assertEquals("Learning XML", new String(events.get(2).getBody()));

        des.reset(); // reset!

        events = des.readEvents(3);
        assertEquals(3, events.size());
        assertEquals("Harry Potter", new String(events.get(0).getBody()));
        assertEquals("XQuery Kick Start", new String(events.get(1).getBody()));
        assertEquals("Learning XML", new String(events.get(2).getBody()));

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

}
