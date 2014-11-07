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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

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

@RunWith(JUnit4.class)
public class RestSourceTest {

    Client client = mock(Client.class);
    WebTarget wt = mock(WebTarget.class);
    Builder builder = mock(Builder.class);
    Response response = mock(Response.class);
    JobExecutionContext context = mock(JobExecutionContext.class);
    Scheduler scheduler = mock(Scheduler.class);
    SchedulerContext schedulerContext = mock(SchedulerContext.class);
    LinkedBlockingQueue<Event> queue ;


    @Before
    public void setUp() throws SchedulerException {
        when(builder.header(anyString(), anyObject())).thenReturn(builder);
        when(wt.request(any(MediaType.class))).thenReturn(builder);
        when(builder.get()).thenReturn(response);
        when(builder.header(anyString(), anyObject())).thenReturn(builder);
        when(wt.request(any(MediaType.class))).thenReturn(builder);
        when(builder.get()).thenReturn(response);
        when(context.getScheduler()).thenReturn(scheduler);
        when(scheduler.getContext()).thenReturn(schedulerContext);
        queue = new LinkedBlockingQueue<Event>(10);
        when(schedulerContext.get("queue")).thenReturn(queue);
        when(schedulerContext.get("client")).thenReturn(client);
    }

   @Test
    public void testSimpleGet() throws JobExecutionException {
        String goodUrl = "GOOD URL";
        when(client.target(goodUrl)).thenReturn(wt);

        //Create Properties
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put(RestSource.CONF_URL, goodUrl);
        properties.put(RestSource.CONF_HEADERS, "{}");

        when(response.readEntity(String.class)).thenReturn("XXXXX");

        //Return headers
        MultivaluedMap<String, String> responseHeaders = new MultivaluedHashMap<String, String>();
        when(response.getStringHeaders()).thenReturn(responseHeaders);
        when(schedulerContext.get("properties")).thenReturn(properties);

        RequestJob job = new RequestJob();
        job.execute(context);

        assertEquals(queue.size(), 1);
        assert(new String(queue.poll().getBody()).equals("XXXXX"));
    }


    @Test
    public void testMultivaluedHeader() throws JobExecutionException {
        String goodUrl = "GOOD URL";
        when(client.target(goodUrl)).thenReturn(wt);

        //Create Properties
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put(RestSource.CONF_URL, goodUrl);
        properties.put(RestSource.CONF_HEADERS, "{}");

        when(response.readEntity(String.class)).thenReturn("XXXXX");

        //Return headers
        MultivaluedMap<String, String> responseHeaders = new MultivaluedHashMap<String, String>();
        responseHeaders.put("GOODHEADER", Arrays.asList("aa", "bb"));
        when(response.getStringHeaders()).thenReturn(responseHeaders);
        when(schedulerContext.get("properties")).thenReturn(properties);

        RequestJob job = new RequestJob();
        job.execute(context);

        assertEquals(queue.size(), 1);
        assertEquals(queue.poll().getHeaders().get("GOODHEADER"), "aa, bb");
    }

}
