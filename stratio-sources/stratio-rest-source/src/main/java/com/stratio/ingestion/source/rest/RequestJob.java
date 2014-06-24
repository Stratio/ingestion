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

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

/**
 * RequestJob. Quartz Job that make a request to a RESTful service.
 * 
 */
public class RequestJob implements Job {

    private static final Logger log = LoggerFactory.getLogger(RequestJob.class);

    public static final String APPLICATION_TYPE = "applicationType";
    public static final String METHOD = "method";
    public static final String URL = "url";
    public static final String BODY_FIELD = "bodyField";

    private Map<String, Object> properties;
    private LinkedBlockingQueue<Event> queue;
    private Client client;

    /**
     * {@inheritDoc}
     * 
     * @param context
     */
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        SchedulerContext schedulerContext = null;
        try {
            schedulerContext = context.getScheduler().getContext();
            initProperties(schedulerContext);

            WebTarget target = client.target((String) properties.get(URL));
            Builder request = setApplicationType(target, (String) properties.get(APPLICATION_TYPE));

            Response response = null;
            switch ((String) properties.get(METHOD)) {
                case "POST":
                    // TODO POST Request.
                    break;
                case "GET":
                    response = request.get();
                    break;
            }

            if (response != null) {
                String responseString = response.readEntity(String.class);
                queue.add(EventBuilder.withBody(responseString, Charsets.UTF_8));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Initialize properties that are received in the {@code SchedulerContext}.
     * 
     * @param context
     */
    @SuppressWarnings("unchecked")
    public void initProperties(SchedulerContext context) {
        queue = (LinkedBlockingQueue<Event>) context.get("queue");
        properties = (Map<String, Object>) context.get("properties");
        client = (Client) context.get("client");
    }

    /**
     * Set an Application Type to the request depending on a parameter and its corresponding
     * {@code MediaType}.
     * 
     * @param targetURL Current target url.
     * @param applicationType ApplicationType to set.
     * @return
     */
    public Builder setApplicationType(WebTarget targetURL, String applicationType) {
        Builder builder = null;
        switch (applicationType) {
            case "TEXT":
                builder = targetURL.request(MediaType.TEXT_PLAIN_TYPE);
                break;
            default:
                builder = targetURL.request(MediaType.APPLICATION_JSON_TYPE);
                break;
        }

        return builder;
    }

}
