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
package com.stratio.ingestion.deserializer.jsonpath;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableFileInputStream;
import org.apache.flume.serialization.ResettableInputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.stratio.ingestion.deserializer.tracker.TransientPositionTracker;

@RunWith(JUnit4.class)
public class JsonpathDeserializerTest {

    private static final String JSON_FILE= "books.json";

    private static final String TITLE="Everyday Italian";
    private static final String YEAR= "2005";
    private static final String CITY= "Barcelona";
    private static final String NUMBER_OF_TITLES= "159";


    private ResettableInputStream getTestInputStream() throws Exception {
        return getTestInputStream(JSON_FILE);
    }

    private ResettableInputStream getTestInputStream(final String path) throws Exception {
        return new ResettableFileInputStream(new File("src/test/resources/" + path), new TransientPositionTracker("dummy"));
    }

    @Test
    public void testReadEvents() throws Exception {
        Context context = new Context();
        context.put("expression", "$.bookstore.books[*]");
        context.put("rootElementsExpression", "$.bookstore");
        context.put("outputBody", "true");
        //context.put("outputHeader", "title");
        EventDeserializer des = new JsonpathDeserializer.Builder().build(context, getTestInputStream());

        List<Event> events= des.readEvents(100);
        assertEquals("Number of expected elements in JSON file", 4, events.size());
        assertEquals("Number of expected elements in Event", 7, events.get(0).getHeaders().size());
        assertEquals(TITLE, events.get(0).getHeaders().get("title"));
        assertEquals(YEAR, events.get(0).getHeaders().get("year"));
        assertEquals(CITY, events.get(0).getHeaders().get("city"));
        assertEquals(NUMBER_OF_TITLES, events.get(0).getHeaders().get("numberOfTitles"));
        assertTrue(new String(events.get(0).getBody()).contains(TITLE));

    }



    @Test(expected = RuntimeException.class)
    public void testBadJSON() throws Exception {
        new JsonpathDeserializer.Builder().build(new Context(), getTestInputStream("bad.json"));
    }




}
