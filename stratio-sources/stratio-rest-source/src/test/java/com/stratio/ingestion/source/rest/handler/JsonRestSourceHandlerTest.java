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
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class JsonRestSourceHandlerTest {

    private RestSourceHandler jsonHandler;

    @Mock
    private Context context;

    @Before
    public void setUp() throws Exception {
        jsonHandler = new JsonRestSourceHandler();
        when(context.getString("jsonPath", JsonRestSourceHandler.DEFAULT_JSON_PATH)).thenReturn("");
        jsonHandler.configure(context);
    }

    @Test
    public void getEventsFromEmptyJson() throws Exception {
        final List<Event> events = jsonHandler
                .getEvents("{}", ImmutableMap.<String,
                        String>builder().build());

        assertThat(events).isNotEmpty().hasSize(1);
        assertThat(new String(events.get(0).getBody())).isEqualTo("{}");
    }

    @Test
    public void getEventsFromNotValidJson() throws Exception {
        final List<Event> events = jsonHandler
                .getEvents("[{\"field1\":\"value1\"},{\"field2\":}]", ImmutableMap.<String,
                        String>builder().build());

        assertThat(events).isEmpty();
    }

    @Test
    public void getEventsWithDefaultPath() throws Exception {
        final List<Event> events = jsonHandler
                .getEvents("[{\"field1\":\"value1\"},{\"field2\":\"value2\"}]", ImmutableMap.<String,
                        String>builder().build());

        assertThat(events).isNotEmpty().hasSize(2);
    }

    @Test
    public void getJustOneEventWithPath() throws Exception {
        when(context.getString("jsonPath", JsonRestSourceHandler.DEFAULT_JSON_PATH)).thenReturn("data");
        jsonHandler.configure(context);
        final List<Event> events = jsonHandler
                .getEvents(
                        "{\"field1\":\"value1\",\"field2\":\"value2\",\"data\":{\"dataField1\":1,\"dataField2\":\"value2\"}}",
                        ImmutableMap.<String,
                                String>builder().build());

        assertThat(events).isNotEmpty().hasSize(1);
        assertThat(new String(events.get(0).getBody())).isEqualTo("{\"dataField1\":1,\"dataField2\":\"value2\"}");

    }

    @Test
    public void getMultipleEventsWithPath() throws Exception {
        when(context.getString("jsonPath", JsonRestSourceHandler.DEFAULT_JSON_PATH)).thenReturn("data");
        jsonHandler.configure(context);
        final List<Event> events = jsonHandler
                .getEvents(
                        "[{\"field1\":\"value1\",\"field2\":\"value2\",\"data\":{\"dataField1\":1,"
                                + "\"dataField2\":\"value1\"}},{\"field1\":\"value1\",\"field2\":\"value2\",\"data\":{\"dataField1\":1,\"dataField2\":\"value2\"}}]",
                        ImmutableMap.<String,
                                String>builder().build());

        assertThat(events).isNotEmpty().hasSize(2);
        assertThat(new String(events.get(0).getBody())).isEqualTo("{\"dataField1\":1,\"dataField2\":\"value1\"}");
        assertThat(new String(events.get(1).getBody())).isEqualTo("{\"dataField1\":1,\"dataField2\":\"value2\"}");

    }
}