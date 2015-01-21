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

import static org.fest.assertions.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class FlumeStatsRestSourceHandlerTest {

    private FlumeStatsRestSourceHandler handler;

    @Mock
    private Context context;

    @Before
    public void setUp() throws Exception {
        handler = new FlumeStatsRestSourceHandler();
        handler.configure(context);
    }

    @Test
    public void getEvents() throws Exception {
      final String response = "{\n"
          + "\"CHANNEL.fileChannel\":{\"EventPutSuccessCount\":\"468085\",\n"
          + "                      \"Type\":\"CHANNEL\"},\n"
          + "\"CHANNEL.memChannel\":{\"EventPutSuccessCount\":\"22948908\",\n"
          + "                   \"Type\":\"CHANNEL\"}"
          + "}";
        final List<Event> events = handler.getEvents(response, ImmutableMap.<String,String>of());
        assertThat(events).isNotEmpty().hasSize(2);
        final Map<String,String> e1 = events.get(0).getHeaders();
        final Map<String,String> e2 = events.get(1).getHeaders();
        assertThat(e1).isEqualTo(ImmutableMap.of("Name", "fileChannel", "Type", "CHANNEL", "EventPutSuccessCount", "468085"));
        assertThat(e2).isEqualTo(ImmutableMap.of("Name", "memChannel", "Type", "CHANNEL", "EventPutSuccessCount", "22948908"));
    }

}