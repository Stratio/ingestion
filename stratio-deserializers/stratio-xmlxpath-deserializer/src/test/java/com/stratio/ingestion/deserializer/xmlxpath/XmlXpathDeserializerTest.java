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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableInputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class XmlXpathDeserializerTest {

    private static final Logger log = LoggerFactory.getLogger(XmlXpathDeserializerTest.class);

    private String xmlContent;

    @Before
    public void setUp() throws IOException {
        URL url = getClass().getResource("/test.xml");
        File file = new File(url.getPath());
        xmlContent = readFile(file);
    }

    @Test
    public void testReadsAndMark() throws IOException {
        Context context = new Context();
        context.put("expression", "/bookstore/book");
        ResettableInputStream in = new ResettableTestStringInputStream(xmlContent);
        EventDeserializer des = new XmlXpathDeserializer(context, in);
        validateReadAndMark(des);
    }

    @Test
    public void testReset() throws IOException {
        Context context = new Context();
        context.put("expression", "/bookstore/book/title");
        ResettableInputStream in = new ResettableTestStringInputStream(xmlContent);
        EventDeserializer des = new XmlXpathDeserializer(context, in);
        validateReset(des);
    }

    private void validateReadAndMark(EventDeserializer des) throws IOException {
        Event evt;

        evt = des.readEvent();
        Assert.assertTrue(new String(evt.getBody()).contains("Giada De Laurentiis"));
        des.mark();

        evt = des.readEvent();
        Assert.assertTrue(new String(evt.getBody()).contains("J K. Rowling"));
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
