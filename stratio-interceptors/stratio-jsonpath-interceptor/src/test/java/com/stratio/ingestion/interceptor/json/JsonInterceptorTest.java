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
package com.stratio.ingestion.interceptor.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
public class JsonInterceptorTest {

    private static final String JSON_FILE= "books.json";
    private static final String JSON_FILE_ORDERS= "orders.json";

    private static final String TITLE="Everyday Italian";
    private static final String YEAR= "2005";
    private static final String CITY= "Barcelona";
    private static final String NUMBER_OF_TITLES= "159";

    private static Map<String, String> TEST_HEADERS= new HashMap<>();
    private static final String TEST_BODY_TEXT= "my text body";
    private static final String TEST_BODY_JSON= "";



    static String readFile(String path) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, StandardCharsets.UTF_8);
    }

    private String getTestInputStream() throws Exception {
        return getTestInputStream(JSON_FILE);
    }

    private String getTestInputStream(final String path) throws Exception {
        return readFile("src/test/resources/" + path);
    }

    @Test
    public void testReadOrders() throws Exception {
        Context context = new Context();
        context.put("jsonHeader", "");
        context.put("expression", "$.lines[*]");
        context.put("overwriteBody", "true");

        JsonInterceptor.Builder builder= new JsonInterceptor.Builder();
        builder.configure(context);
        JsonInterceptor interceptor= (JsonInterceptor) builder.build();

        HashMap<String, String> orderHeaders= new HashMap<>();
        orderHeaders.put("timestamp", "2016-03-17 17:51:25");
        orderHeaders.put("payment_method", "cash");
        orderHeaders.put("credit_card", "4079962470600654");
        orderHeaders.put("shopping_center", "Valencia");
        orderHeaders.put("employee", "57");

        List<Event> inputEvents= new ArrayList<>();
        Event testEvent1= EventBuilder.withBody(getTestInputStream(JSON_FILE_ORDERS),
                StandardCharsets.UTF_8, orderHeaders);
        inputEvents.add(testEvent1);

        List<Event> events= interceptor.intercept(inputEvents);
        assertEquals("Number of expected elements in JSON file", 4, events.size());
        assertEquals("Number of expected elements in Event", 9, events.get(0).getHeaders().size());
        assertEquals("cash", events.get(0).getHeaders().get("payment_method"));
        assertEquals("Valencia", events.get(0).getHeaders().get("shopping_center"));
        assertEquals("electronic", events.get(0).getHeaders().get("family"));
        //assertEquals(NUMBER_OF_TITLES, events.get(0).getHeaders().get("numberOfTitles"));
        assertTrue(new String(events.get(0).getBody()).contains("electronic"));

    }

    @Test
    public void testReadEvents() throws Exception {
        Context context = new Context();
        context.put("jsonHeader", "books");
        context.put("expression", "$.bookstore.books[*]");
        context.put("overwriteBody", "true");

        JsonInterceptor.Builder builder= new JsonInterceptor.Builder();
        builder.configure(context);
        JsonInterceptor interceptor= (JsonInterceptor) builder.build();

        List<Event> inputEvents= new ArrayList<>();
        Event testEvent1= getTestEvent(TEST_BODY_TEXT, TEST_HEADERS);
        testEvent1.getHeaders().put("books", getTestInputStream());
        inputEvents.add(testEvent1);

        List<Event> events= interceptor.intercept(inputEvents);
        assertEquals("Number of expected elements in JSON file", 4, events.size());
        assertEquals("Number of expected elements in Event", 7, events.get(0).getHeaders().size());
        assertEquals(TITLE, events.get(0).getHeaders().get("title"));
        assertEquals(YEAR, events.get(0).getHeaders().get("year"));
        //assertEquals(NUMBER_OF_TITLES, events.get(0).getHeaders().get("numberOfTitles"));
        assertTrue(new String(events.get(0).getBody()).contains(TITLE));

    }

    @Test
    public void testReadFromBody() throws Exception {
        Context context = new Context();
        //context.put("jsonHeader", "books");
        context.put("expression", "$.bookstore.books[*]");
        context.put("overwriteBody", "false");

        JsonInterceptor.Builder builder= new JsonInterceptor.Builder();
        builder.configure(context);
        JsonInterceptor interceptor= (JsonInterceptor) builder.build();

        List<Event> inputEvents= new ArrayList<>();
        Event testEvent1= getTestEvent(TEST_BODY_TEXT, TEST_HEADERS);
        testEvent1.setBody(getTestInputStream().getBytes());
        //testEvent1.getHeaders().put("books", getTestInputStream());
        inputEvents.add(testEvent1);

        List<Event> events= interceptor.intercept(inputEvents);
        assertEquals("Number of expected elements in JSON file", 4, events.size());
        assertEquals("Number of expected elements in Event", 7, events.get(0).getHeaders().size());
        assertEquals(TITLE, events.get(0).getHeaders().get("title"));
        assertEquals(YEAR, events.get(0).getHeaders().get("year"));
    }

    @Test
    public void testNotModifiedBody() throws Exception {
        Context context = new Context();
        context.put("jsonHeader", "books");
        context.put("expression", "$.bookstore.books[*]");
        context.put("overwriteBody", "false");

        JsonInterceptor.Builder builder= new JsonInterceptor.Builder();
        builder.configure(context);
        JsonInterceptor interceptor= (JsonInterceptor) builder.build();

        List<Event> inputEvents= new ArrayList<>();
        Event testEvent1= getTestEvent(TEST_BODY_TEXT, TEST_HEADERS);
        testEvent1.getHeaders().put("books", getTestInputStream());
        inputEvents.add(testEvent1);

        List<Event> events= interceptor.intercept(inputEvents);
        assertEquals("Number of expected elements in JSON file", 4, events.size());
        assertEquals(TEST_BODY_TEXT, new String(events.get(0).getBody()));
    }

    @Test
    public void testBadJsonEvents() throws Exception {
        Context context = new Context();
        context.put("jsonHeader", "books");
        context.put("expression", "$.bookstore.books[*]");
        context.put("overwriteBody", "true");

        JsonInterceptor.Builder builder = new JsonInterceptor.Builder();
        builder.configure(context);
        JsonInterceptor interceptor = (JsonInterceptor) builder.build();

        List<Event> inputEvents = new ArrayList<>();
        inputEvents.add( getTestEvent(TEST_BODY_TEXT, TEST_HEADERS));

        List<Event> events= interceptor.intercept(inputEvents);
        assertEquals("Number of expected elements in JSON file", 0, events.size());
    }

    private Event getTestEvent(String body, Map<String, String> headers)    {
        return EventBuilder.withBody(body, StandardCharsets.UTF_8, headers);
    }

    @BeforeClass
    public static void setup() {
        TEST_HEADERS.put("header1", "value1");
        TEST_HEADERS.put("header2", "value2");
        TEST_HEADERS.put("books", "text value");
    }


}
