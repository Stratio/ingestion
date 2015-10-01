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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import javax.net.ssl.SSLContext;
import javax.ws.rs.core.MediaType;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.quartz.SchedulerContext;
import org.quartz.SchedulerException;

import com.stratio.ingestion.source.rest.handler.RestSourceHandler;
import com.stratio.ingestion.source.rest.url.UrlHandler;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import junit.framework.TestCase;
/**
 * Created by miguelsegura on 29/09/15.
 */
@RunWith(JUnit4.class)
public class RestSourceTest extends TestCase {
//    RequestJob job = spy(new RequestJob());
    RestSource rest = spy(new RestSource());
    Client client = mock(Client.class);
    WebResource webResource = mock(WebResource.class);
    ClientResponse response = mock(ClientResponse.class);
    JobExecutionContext contextJob = mock(JobExecutionContext.class);
    Scheduler scheduler = mock(Scheduler.class);
    SchedulerContext schedulerContext = mock(SchedulerContext.class);
    LinkedBlockingQueue<Event> queue;
    WebResource.Builder builder = mock(WebResource.Builder.class);
    RestSourceHandler restSourceHandler;
    UrlHandler urlHandler;
    Context contextSource = mock(Context.class);
    private Map<String, String> properties = mock(HashMap.class);
    SSLContext ctx = mock(SSLContext.class);

    private static final Integer DEFAULT_FREQUENCY = 10;
    private static final String DEFAULT_JOBNAME = "Quartz Job";
    private static final String DEFAULT_METHOD = "GET";
    private static final String DEFAULT_APPLICATION_TYPE = "JSON";
    private static final String DEFAULT_HEADERS = "{}";
    private static final String DEFAULT_BODY = "";

    protected static final String CONF_FREQUENCY = "frequency";
    protected static final String CONF_URL = "url";
    protected static final String CONF_METHOD = "method";
    protected static final String CONF_APPLICATION_TYPE = "applicationType";
    protected static final String CONF_HEADERS = "headers";
    protected static final String CONF_BODY = "body";
    protected static final String CONF_HANDLER = "restSourceHandler";
    protected static final String DEFAULT_REST_HANDLER = "com.stratio.ingestion.source.rest.handler"
            + ".DefaultRestSourceHandler";
    protected static final String CONF_SKIP_SSL = "skipSsl";
    protected static final String URL_HANDLER = "urlHandler";
    protected static final String DEFAULT_URL_HANDLER = "com.stratio.ingestion.source.rest.url."
            + "DefaultUrlHandler";
    protected static final String URL_CONF = "urlHandlerConfig";

    @Before
    public void setUp() throws SchedulerException {
        when(builder.header(anyString(), anyObject())).thenReturn(builder);
        when(builder.header(anyString(), anyObject())).thenReturn(builder);
        when(builder.get(ClientResponse.class)).thenReturn(response);
        when(webResource.accept(any(MediaType.class))).thenReturn(builder);
        queue = new LinkedBlockingQueue<Event>(10);
        when(schedulerContext.get("queue")).thenReturn(queue);
        when(schedulerContext.get("client")).thenReturn(client);
        when(contextJob.getScheduler()).thenReturn(scheduler);
        when(scheduler.getContext()).thenReturn(schedulerContext);
        when(contextSource.getInteger("frequency", 10)).thenReturn(10);
        when(contextSource.getString(CONF_URL)).thenReturn(CONF_URL);
        when(contextSource.getString(CONF_METHOD, DEFAULT_METHOD)).thenReturn(DEFAULT_METHOD);
        when(contextSource.getString(CONF_APPLICATION_TYPE, DEFAULT_APPLICATION_TYPE)).thenReturn
                (DEFAULT_APPLICATION_TYPE);
        when(contextSource.getString(CONF_HEADERS, DEFAULT_HEADERS)).thenReturn(DEFAULT_HEADERS);
        when(contextSource.getString(CONF_BODY, DEFAULT_BODY)).thenReturn(DEFAULT_BODY);
        when(contextSource.getString(CONF_HANDLER, DEFAULT_REST_HANDLER)).thenReturn(DEFAULT_REST_HANDLER);
        when(contextSource.getString(URL_CONF)).thenReturn(URL_CONF);
        when(contextSource.getString(URL_HANDLER, DEFAULT_URL_HANDLER)).thenReturn(DEFAULT_URL_HANDLER);
        when(contextSource.getBoolean(CONF_SKIP_SSL, Boolean.FALSE)).thenReturn(Boolean.TRUE);


    }

    @Test
    public void checkProcess() throws EventDeliveryException {

        rest.configure(contextSource);
        rest.start();
        PollableSource.Status status = rest.process();
        rest.stop();
    }

    @Test
    public void checkInitClient() throws EventDeliveryException {
        rest.configure(contextSource);
    }

    @Test
    public void checkInitRestSource() throws EventDeliveryException {
        RestSource restS = new RestSource(client);
    }

    @Test
    public void checkStartFailure() throws EventDeliveryException {
        when(schedulerContext.get("client")).thenReturn(null);
        rest.start();
    }

    @Test
    public void checkUrlHandlerFailure() throws EventDeliveryException {
        when(contextSource.getString(URL_HANDLER, DEFAULT_URL_HANDLER)).thenReturn("urlHandler");
        rest.configure(contextSource);
        rest.start();
    }

    @Test
    public void checkRestSourceHandlerFailure() throws EventDeliveryException {
        when(contextSource.getString(CONF_HANDLER, DEFAULT_REST_HANDLER)).thenReturn("RestSourceHandler");
        rest.configure(contextSource);
        rest.start();
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkPollFailure() throws EventDeliveryException {
        when(contextSource.getInteger("frequency", 10)).thenReturn(-1);
        rest.configure(contextSource);
        rest.start();
    }

    @Test(expected = NullPointerException.class)
    public void checkStopFailure() throws NoSuchAlgorithmException {
        rest.stop();
    }

    public void testConfigure() throws Exception {

    }

    public void testStart() throws Exception {

    }

    public void testProcess() throws Exception {

    }

    public void testStop() throws Exception {

    }
}