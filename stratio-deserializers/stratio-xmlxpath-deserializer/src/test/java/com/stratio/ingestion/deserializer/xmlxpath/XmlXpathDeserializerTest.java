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

@RunWith(JUnit4.class)
public class XmlXpathDeserializerTest {

    private static final Logger log = LoggerFactory.getLogger(XmlXpathDeserializerTest.class);

    private InputStream getTestInputStream() {
        return getClass().getResourceAsStream("/test.xml");
    }

    @Test
    public void testReadsAndMark() throws IOException {
        Context context = new Context();
        context.put("expression", "/bookstore/book");
        EventDeserializer des = new XmlXpathDeserializer(context, getTestInputStream());
        validateReadAndMark(des);
    }

    @Test
    public void testReset() throws IOException {
        Context context = new Context();
        context.put("expression", "/bookstore/book/title");
        EventDeserializer des = new XmlXpathDeserializer(context, getTestInputStream());
        validateReset(des);
    }

    @Test
    public void testDocument2String() throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = factory.newDocumentBuilder();
        InputSource is = new InputSource(getTestInputStream());
        Document doc = docBuilder.parse(is);

        Context context = new Context();
        context.put("expression", "/bookstore/book/title");
        XmlXpathDeserializer des = new XmlXpathDeserializer(context, getTestInputStream());

        Assert.assertNotNull(des.document2String(doc));
        des.close();
    }

    @Test
    public void testXPathStaticHeaders() throws IOException {
        Context context = new Context();
        context.put("expression", "/bookstore/book");
        context.put("headers.book", "/bookstore/book[@category='CHILDREN']/title");
        context.put("headers.author", "/bookstore/book[@category='CHILDREN']/author");
        EventDeserializer des = new XmlXpathDeserializer(context, getTestInputStream());
        validateHeaders(des);
    }

    private void validateReadAndMark(EventDeserializer des) throws IOException {
        Event evt;

        evt = des.readEvent();
        Assert.assertTrue(new String(evt.getHeaders().get("element"))
                .contains("Giada De Laurentiis"));
        des.mark();

        evt = des.readEvent();
        Assert.assertTrue(new String(evt.getHeaders().get("element")).contains("J K. Rowling"));
        des.mark(); // reset!

        List<Event> readEvents = des.readEvents(2);
        Assert.assertTrue(readEvents.size() == 2);

        evt = des.readEvent();
        Assert.assertNull("Event should be null because there are no more books " + "left to read",
                evt);

        des.mark();
        des.mark();
        des.close();
    }

    private void validateReset(EventDeserializer des) throws IOException {
        Event evt;

        des.readEvent();
        des.mark();

        des.readEvents(3);
        des.reset(); // reset!

        List<Event> readEvents = des.readEvents(3);
        Assert.assertTrue(readEvents.size() == 3);
        des.mark();
        for (Event e : readEvents) {
            Assert.assertNotNull(e);
        }

        evt = des.readEvent();
        Assert.assertNull("Event should be null because there are no more books " + "left to read",
                evt);

    }

    private void validateHeaders(EventDeserializer des) throws IOException {
        List<Event> events = des.readEvents(4);
        Assert.assertTrue(events.size() == 4);

        for (Event evt : events) {
            Assert.assertEquals(evt.getHeaders().get("author"), "J K. Rowling");
        }
    }

    private String readFile(File file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        try {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append("\n");
                line = br.readLine();
            }
            return sb.toString();
        } finally {
            br.close();
        }
    }

}
