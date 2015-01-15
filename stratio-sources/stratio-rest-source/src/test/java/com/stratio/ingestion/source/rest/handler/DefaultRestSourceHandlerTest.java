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
package com.stratio.ingestion.source.rest.handler;

import java.util.List;

import org.apache.flume.Event;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class DefaultRestSourceHandlerTest {

    private RestSourceHandler handler;

    @Before
    public void setUp() throws Exception {
        handler = new DefaultRestSourceHandler();

    }

    @Test
    public void getEventsWithEmptyBodyAndEmptyHeaders() {
        final List<Event> events = handler.getEvents("", new ImmutableMap.Builder<String, String>().build());
        Assertions.assertThat(events).isNotNull();
    }

    @Test
    public void getEventsWithBodyAndHeaders() throws Exception {
        final ImmutableMap<String, String> headersMap = new ImmutableMap.Builder<String, String>().put
                ("header1", "value1").put
                ("header2", "value2").build();
        final List<Event> events = handler.getEvents("body", headersMap);
        Assertions.assertThat(events).isNotNull();
        Assertions.assertThat(events.get(0).getBody()).isEqualTo("body".getBytes());
        Assertions.assertThat(events.get(0).getHeaders()).isEqualTo(headersMap);

    }

    @Test
    public void getEventsWithBodyAndHeadersAndPath() throws Exception {
        final ImmutableMap<String, String> headersMap = new ImmutableMap.Builder<String, String>().put
                ("header1", "value1").put
                ("header2", "value2").build();
        final List<Event> events = handler.getEvents("body", headersMap);
        Assertions.assertThat(events).isNotNull();
        Assertions.assertThat(events.get(0).getBody()).isEqualTo("body".getBytes());
        Assertions.assertThat(events.get(0).getHeaders()).isEqualTo(headersMap);

    }
}