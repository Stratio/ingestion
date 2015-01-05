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
package com.stratio.ingestion.source.batch.rest.request;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.flume.Event;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.core.util.MultivaluedMapImpl;

@RunWith(MockitoJUnitRunner.class)
public class RequestJobTest {

    public static final String DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ssXXX";
    @InjectMocks
    private RequestJob requestJob;

    @Mock
    ClientResponse response;

    @Mock
    LinkedBlockingQueue<Event> queue;

    @Mock
    Map<String, String> properties;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

    }

    @Test
    public void addEventsToQueueWithNonSetLastEventProperty() throws Exception {
        final String responseAsString =
                "[{\"version\":1,\"date\":\"2014-12-16T16:32:33+00:00\",\"data\":{\"name\":\"John\",\"surname\":\"Doe\"}},"
                        + "{\"version\":1,\"date\":\"2014-12-16T16:32:33+00:00\",\"data\":{\"name\":\"John\",\"surname\":\"Doe\"}},{\"version\":1,"
                        + "\"date\":\"2014-12-16T16:32:33+00:00\",\"data\":{\"name\":\"John\",\"surname\":\"Doe\"}}]";
        when(response.getStatus()).thenReturn(200);
        when(response.getEntity(String.class)).thenReturn(responseAsString);
        when(queue.add(any(Event.class))).thenReturn(Boolean.TRUE).thenReturn(Boolean.TRUE).thenReturn(Boolean.TRUE);
        when(properties.get("checkpointValue")).thenReturn(null, null, null, null);
        when(properties.get("datePattern")).thenReturn("yyyy-MM-dd'T'HH:mm:ssXXX");
        when(properties.get("checkpointEnabled")).thenReturn("true", "true", "true", "true");
        mockHeaders();

        requestJob.addEventsToQueue(response);

        verify(queue, times(4)).add(any(Event.class));
        verify(properties).put(eq("checkpointValue"), any(String.class));
    }

    @Test
    public void addJustOneOfThreeEventsToQueueWithSetLastEventProperty() throws Exception {
        final String responseAsString =
                "[{\"version\":1,\"date\":\"2014-12-16T16:32:33+00:00\",\"data\":{\"name\":\"John\",\"surname\":\"Doe\"}},"
                        + "{\"version\":1,\"date\":\"2014-12-17T16:32:33+00:00\",\"data\":{\"name\":\"John\","
                        + "\"surname\":\"Doe\"}},{\"version\":1,"
                        + "\"date\":\"2014-12-19T16:32:33+00:00\",\"data\":{\"name\":\"John\",\"surname\":\"Doe\"}}]";
        when(response.getStatus()).thenReturn(200);
        when(response.getEntity(String.class)).thenReturn(responseAsString);
        when(queue.add(any(Event.class))).thenReturn(Boolean.TRUE).thenReturn(Boolean.TRUE).thenReturn(Boolean.TRUE);
        when(properties.get("checkpointValue"))
                .thenReturn("2014-12-18T16:32:33+00:00",
                        "2014-12-18T16:32:33+00:00",
                        "2014-12-18T16:32:33+00:00",
                        "2014-12-18T16:32:33+00:00");
        when(properties.get("datePattern")).thenReturn(DATE_PATTERN);
        when(properties.get("checkpointEnabled")).thenReturn("true", "true", "true", "true");
        mockHeaders();

        requestJob.addEventsToQueue(response);

        verify(queue, times(2)).add(any(Event.class));
        verify(properties, times(1)).put(eq("checkpointValue"), eq("2014-12-19T17:32:33+01:00"));

    }

    @Test
    public void addNoOneOfThreeEventsToQueueWithSetLastEventProperty() throws Exception {
        final String responseAsString =
                "[{\"version\":1,\"date\":\"2014-12-16T16:32:33+00:00\",\"data\":{\"name\":\"John\",\"surname\":\"Doe\"}},"
                        + "{\"version\":1,\"date\":\"2014-12-17T16:32:33+00:00\",\"data\":{\"name\":\"John\","
                        + "\"surname\":\"Doe\"}},{\"version\":1,"
                        + "\"date\":\"2014-12-17T16:32:33+00:00\",\"data\":{\"name\":\"John\",\"surname\":\"Doe\"}}]";
        when(response.getStatus()).thenReturn(200);
        when(response.getEntity(String.class)).thenReturn(responseAsString);
        when(queue.add(any(Event.class))).thenReturn(Boolean.TRUE).thenReturn(Boolean.TRUE).thenReturn(Boolean.TRUE);
        when(properties.get("checkpointValue"))
                .thenReturn("2014-12-18T16:32:33+00:00",
                        "2014-12-18T16:32:33+00:00",
                        "2014-12-18T16:32:33+00:00",
                        "2014-12-18T16:32:33+00:00");
        when(properties.get("datePattern")).thenReturn(DATE_PATTERN);
        when(properties.get("checkpointEnabled")).thenReturn("true", "true", "true", "true");
        mockHeaders();

        requestJob.addEventsToQueue(response);

        verify(queue, times(0)).add(any(Event.class));
        verify(properties, times(0)).put(eq("checkpointValue"), eq("2014-12-17T16:32:33+00:00"));

    }

    private String getTimeFromStringDate(String dateAsString) {
        String time = null;
        try {
            time = String.valueOf(new SimpleDateFormat(DATE_PATTERN).parse(dateAsString).getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return time;
    }

    private void mockHeaders() {
        MultivaluedMap<String, String> headers = new MultivaluedMapImpl();
        headers.putSingle("Accept", "application/json");
        when(response.getHeaders()).thenReturn(headers).thenReturn(headers).thenReturn(headers);
    }
}