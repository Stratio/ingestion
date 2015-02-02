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
package com.stratio.ingestion.source.rest;

import static org.fest.assertions.Assertions.assertThat;
import static org.fest.assertions.MapAssert.entry;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerContext;
import org.quartz.SchedulerException;

import com.stratio.ingestion.source.rest.handler.DefaultRestSourceHandler;
import com.stratio.ingestion.source.rest.handler.JsonRestSourceHandler;
import com.stratio.ingestion.source.rest.handler.RestSourceHandler;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;

@RunWith(JUnit4.class)
public class RestJobTest {
    RequestJob job = spy(new RequestJob());
    Client client = mock(Client.class);
    WebResource webResource = mock(WebResource.class);
    ClientResponse response = mock(ClientResponse.class);
    JobExecutionContext context = mock(JobExecutionContext.class);
    Scheduler scheduler = mock(Scheduler.class);
    SchedulerContext schedulerContext = mock(SchedulerContext.class);
    LinkedBlockingQueue<Event> queue;
    WebResource.Builder builder = mock(WebResource.Builder.class);
    RestSourceHandler handler;

    @Before
    public void setUp() throws SchedulerException {
        when(builder.header(anyString(), anyObject())).thenReturn(builder);
        when(builder.header(anyString(), anyObject())).thenReturn(builder);
        when(builder.get(ClientResponse.class)).thenReturn(response);
        when(webResource.accept(any(MediaType.class))).thenReturn(builder);
        when(context.getScheduler()).thenReturn(scheduler);
        when(scheduler.getContext()).thenReturn(schedulerContext);
        queue = new LinkedBlockingQueue<Event>(10);
        when(schedulerContext.get("queue")).thenReturn(queue);
        when(schedulerContext.get("client")).thenReturn(client);

    }

    @Test
    public void simpleGetUsingDefaultHandler() throws JobExecutionException {
        String goodUrl = "GOOD URL";
        MultivaluedMap<String, String> responseHeaders = new MultivaluedMapImpl();
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put(RestSource.CONF_URL, goodUrl);
        properties.put(RestSource.CONF_HEADERS, "{}");
        properties.put(RestSource.CONF_METHOD, "GET");
        when(client.resource(goodUrl)).thenReturn(webResource);
        when(response.getEntity(String.class)).thenReturn("XXXXX");
        when(response.getHeaders()).thenReturn(responseHeaders);
        when(schedulerContext.get("properties")).thenReturn(properties);
        handler = initDefaultHandler();
        when(schedulerContext.get("handler")).thenReturn(handler);

        RequestJob job = new RequestJob();
        job.execute(context);

        assertThat(queue.size()).isEqualTo(1);
        assertThat(new String(queue.poll().getBody()).equals("XXXXX"));
    }

    private RestSourceHandler initDefaultHandler() {
        RestSourceHandler defaultHandler = new DefaultRestSourceHandler();
        return defaultHandler;
    }

    @Test
    public void simpleGetWithJustOneEventUsingJsonHandler() throws JobExecutionException {
        String goodUrl = "GOOD URL";
        Map<String, Object> properties = new HashMap<String, Object>();
        MultivaluedMap<String, String> responseHeaders = new MultivaluedMapImpl();
        properties.put(RestSource.CONF_URL, goodUrl);
        properties.put(RestSource.CONF_HEADERS, "{}");
        properties.put(RestSource.CONF_METHOD, "GET");
        String jsonResponse = "{\"field1\": \"value1\"}";

        when(client.resource(goodUrl)).thenReturn(webResource);
        when(response.getEntity(String.class)).thenReturn(jsonResponse);
        when(response.getHeaders()).thenReturn(responseHeaders);
        when(schedulerContext.get("properties")).thenReturn(properties);
        handler = initJsonHandler("");
        when(schedulerContext.get("handler")).thenReturn(handler);

        job.execute(context);

        assertThat(queue.size()).isEqualTo(1);
        assertThat(new String(queue.poll().getBody())).isEqualTo("{\"field1\":\"value1\"}");
    }

    private RestSourceHandler initJsonHandler(String pathValue) {
        RestSourceHandler jsonHandler = new JsonRestSourceHandler();
        Context flumeContext = new Context();
        flumeContext.put("jsonPath", pathValue);
        jsonHandler.configure(flumeContext);
        return jsonHandler;
    }

    @Test
    public void simpleGetWithMultipleEventsUsingJsonHandler() throws JobExecutionException {
        String goodUrl = "GOOD URL";
        Map<String, Object> properties = new HashMap<String, Object>();
        MultivaluedMap<String, String> responseHeaders = new MultivaluedMapImpl();
        properties.put(RestSource.CONF_URL, goodUrl);
        properties.put(RestSource.CONF_HEADERS, "{}");
        properties.put(RestSource.CONF_METHOD, "GET");
        String jsonResponse = "[{\"field1\":\"value1\"},{\"field2\":\"value2\"}]";

        when(client.resource(goodUrl)).thenReturn(webResource);
        when(response.getEntity(String.class)).thenReturn(jsonResponse);
        when(response.getHeaders()).thenReturn(responseHeaders);
        when(schedulerContext.get("properties")).thenReturn(properties);
        handler = initJsonHandler("");
        when(schedulerContext.get("handler")).thenReturn(handler);

        job.execute(context);

        assertThat(queue.size()).isEqualTo(2);
        assertThat(new String(queue.poll().getBody())).isEqualTo("{\"field1\":\"value1\"}");
        assertThat(new String(queue.poll().getBody())).isEqualTo("{\"field2\":\"value2\"}");
    }

    @Test
    public void simpleGetWithMultipleEventsAndHeadersUsingJsonHandler() throws JobExecutionException {
        String goodUrl = "GOOD URL";
        Map<String, Object> properties = new HashMap<String, Object>();
        MultivaluedMap<String, String> responseHeaders = new MultivaluedMapImpl();
        responseHeaders.put("header1", Arrays.asList("value1", "value2"));
        properties.put(RestSource.CONF_URL, goodUrl);
        properties.put(RestSource.CONF_HEADERS, "{}");
        properties.put(RestSource.CONF_METHOD, "GET");
        String jsonResponse = "[{\"field1\":\"value1\"},{\"field2\":\"value2\"}]";

        when(client.resource(goodUrl)).thenReturn(webResource);
        when(response.getEntity(String.class)).thenReturn(jsonResponse);
        when(response.getHeaders()).thenReturn(responseHeaders);
        when(schedulerContext.get("properties")).thenReturn(properties);
        handler = initJsonHandler("");
        when(schedulerContext.get("handler")).thenReturn(handler);

        job.execute(context);

        assertThat(queue.size()).isEqualTo(2);
        Event poll = queue.poll();
        assertThat(new String(poll.getBody())).isEqualTo("{\"field1\":\"value1\"}");
        assertThat(poll.getHeaders()).includes(entry("header1", "value1, value2"));
        poll = queue.poll();
        assertThat(new String(poll.getBody())).isEqualTo("{\"field2\":\"value2\"}");
        assertThat(poll.getHeaders()).includes(entry("header1", "value1, value2"));
    }

    @Test
    public void simpleGetWithMultipleEventsAndHeadersUsingJsonHandlerAndPath() throws JobExecutionException {
        String goodUrl = "GOOD URL";
        Map<String, Object> properties = new HashMap<String, Object>();
        MultivaluedMap<String, String> responseHeaders = new MultivaluedMapImpl();
        responseHeaders.put("header1", Arrays.asList("value1", "value2"));
        properties.put(RestSource.CONF_URL, goodUrl);
        properties.put(RestSource.CONF_HEADERS, "{}");
        properties.put(RestSource.CONF_METHOD, "GET");
        String jsonResponse = "[{\"field1\":\"value1\",\"field2\":\"value2\",\"data\":{\"dataField1\":1,"
                + "\"dataField2\":\"value1\"}},{\"field1\":\"value1\",\"field2\":\"value2\",\"data\":{\"dataField1\":1,\"dataField2\":\"value2\"}}]";

        when(client.resource(goodUrl)).thenReturn(webResource);
        when(response.getEntity(String.class)).thenReturn(jsonResponse);
        when(response.getHeaders()).thenReturn(responseHeaders);
        when(schedulerContext.get("properties")).thenReturn(properties);
        handler = initJsonHandler("data");
        when(schedulerContext.get("handler")).thenReturn(handler);
        
        job.execute(context);

        assertThat(queue.size()).isEqualTo(2);
        Event poll = queue.poll();
        assertThat(new String(poll.getBody())).isEqualTo("{\"dataField1\":1,\"dataField2\":\"value1\"}");
        assertThat(poll.getHeaders()).includes(entry("header1", "value1, value2"));
        poll = queue.poll();
        assertThat(new String(poll.getBody())).isEqualTo("{\"dataField1\":1,\"dataField2\":\"value2\"}");
        assertThat(poll.getHeaders()).includes(entry("header1", "value1, value2"));
    }

    @Test
    public void testMultivaluedHeader() throws JobExecutionException {
        String goodUrl = "GOOD URL";
        when(client.resource(goodUrl)).thenReturn(webResource);

        //Create Properties
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put(RestSource.CONF_URL, goodUrl);
        properties.put(RestSource.CONF_HEADERS, "{}");
        properties.put(RestSource.CONF_METHOD, "GET");
        when(response.getEntity(String.class)).thenReturn("XXXXX");

        //Return headers
        MultivaluedMap<String, String> responseHeaders = new MultivaluedMapImpl();
        responseHeaders.put("GOODHEADER", Arrays.asList("aa", "bb"));
        when(response.getHeaders()).thenReturn(responseHeaders);
        when(schedulerContext.get("properties")).thenReturn(properties);
        handler = initDefaultHandler();
        when(schedulerContext.get("handler")).thenReturn(handler);

        job.execute(context);

        assertEquals(queue.size(), 1);
        assertEquals(queue.poll().getHeaders().get("GOODHEADER"), "aa, bb");
    }

}
