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
package com.stratio.ingestion.sink.druid;

import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.fest.assertions.Assertions;
import org.junit.Test;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;

/**
 * Created by eambrosio on 15/04/15.
 */
public class EventParserTest {

    private EventParser eventParser = new EventParser("timestamp");




    @Test
    public void parseValidEvents() throws Exception {
        List<Event> events = buildEvents();
        final List<Map<String, Object>> parsedEvents = eventParser.parse(events);
        assertThat(parsedEvents).isNotNull();
        assertThat(parsedEvents.size()).isEqualTo(2);

    }

    @Test
    public void parseEmptyEvents() throws Exception {
        List<Event> events = new ArrayList<Event>();
        final List<Map<String, Object>> parsedEvents =  eventParser.parse(events);
        assertThat(parsedEvents).isNotNull();
        assertThat(parsedEvents.size()).isEqualTo(0);

    }

    @Test
    public void parseInvalidEvents() {
        Assertions.assertThat(eventParser.parse(null)).isEmpty();

    }

    private List<Event> buildEvents() {
        List<Event> events = new ArrayList<Event>();
        events.add(createEvent("0"));
        events.add(createEvent("1"));

        return events;
    }

    private Event createEvent(String index) {
        ObjectNode jsonBody = new ObjectNode(JsonNodeFactory.instance);
        jsonBody.put("field1" + index, "foo");
        jsonBody.put("field2" + index, 32);

        Map<String, String> headers = new HashMap<String, String>();
        headers.put("header1" + index, "bar");
        headers.put("header2" + index, "64");
        headers.put("header3" + index, "true");
        headers.put("header4" + index, "1.0");
        headers.put("header5" + index, null);

        return EventBuilder.withBody(jsonBody.toString().getBytes(Charsets.UTF_8), headers);
    }


}