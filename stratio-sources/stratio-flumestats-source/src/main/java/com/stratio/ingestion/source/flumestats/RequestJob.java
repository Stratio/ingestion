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
package com.stratio.ingestion.source.flumestats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.flume.Event;
import org.apache.flume.event.JSONEvent;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RequestJob. Quartz Job that make a request to a RESTful service.
 * 
 */
public class RequestJob implements Job {

    private static final Logger log = LoggerFactory.getLogger(RequestJob.class);

    private LinkedBlockingQueue<Event> queue;
    private Client client;
    private String url;
    private ObjectMapper mapper = new ObjectMapper();

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

            Response response = client.target(url).request(MediaType.APPLICATION_JSON_TYPE).get();

            if (response != null) {
                List<Event> eventList = processStatsResponse(response);
                for (Event e : eventList) {
                    queue.put(e);
                }
            }

        } catch (Exception e) {
            log.error("Can't access to metrics at " + url);
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
        url = (String) context.get("url");
        client = (Client) context.get("client");
    }

    /**
     * Process Flume Stats response and add each component document to a list.
     * 
     * @param response {@code Response} from request.
     * @return A list of {@code Event}, each one contains information from a flume component.
     * @throws JsonParseException
     * @throws JsonMappingException
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public List<Event> processStatsResponse(Response response) throws JsonParseException,
            JsonMappingException, IOException {
       
        String responseString = response.readEntity(String.class);
        Map<String, Object> map = mapper.readValue(responseString, Map.class);
        List<Event> list = new ArrayList<Event>();

        for (Entry<String, Object> entry : map.entrySet()) {

            Map<String, String> headers = new HashMap<String, String>();
            String key = entry.getKey();
            String name = key.substring(key.indexOf(".") + 1, key.length());
            headers.put("Name", name);

            for (Entry<String, Object> componentInfo : ((Map<String, Object>) entry.getValue())
                    .entrySet()) {
                headers.put(componentInfo.getKey(), componentInfo.getValue().toString());
            }

            Event event = new JSONEvent();
            event.setHeaders(headers);
            event.setBody("".getBytes());
            
            
            list.add(event);
        }

        return list;
    }

}
