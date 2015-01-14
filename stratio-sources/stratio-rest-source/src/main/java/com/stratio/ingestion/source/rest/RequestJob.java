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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.flume.Event;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.ingestion.source.rest.handler.RestSourceHandler;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * RequestJob. Quartz Job that make a request to a RESTful service.
 */
public class RequestJob implements Job {

    private static final Logger log = LoggerFactory.getLogger(RequestJob.class);

    public static final String APPLICATION_TYPE = "applicationType";
    public static final String METHOD = "method";
    public static final String URL = "url";
    public static final String HEADERS = "headers";
    public static final String DEFAULT_REST_SOURCE_HANDLER = "com.stratio.ingestion.source"
            + ".rest.DefaultRestSourceHandler";
    public static final String HANDLER = "handler";

    private Map<String, String> properties;
    private LinkedBlockingQueue<Event> queue;
    private Client client;
    private MediaType mediaType;
    private JobExecutionContext context;
    private RestSourceHandler handler;

    /**
     * {@inheritDoc}
     *
     * @param context
     */
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        this.context = context;
        SchedulerContext schedulerContext = null;
        try {
            log.debug("Executing quartz job");
            schedulerContext = context.getScheduler().getContext();
            initProperties(schedulerContext);

            WebResource.Builder resourceBuilder = getBuilder();
            ClientResponse response = getResponse(resourceBuilder);

            if (response != null) {
                String responseString = response.getEntity(String.class);
                queue.addAll(handler.getEvents(responseString, responseToHeaders(response.getHeaders())));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private WebResource.Builder getBuilder() {
        WebResource resource = client.resource(properties.get(URL));
        WebResource.Builder resourceBuilder = setApplicationType(resource, properties.get(APPLICATION_TYPE));
        addHeaders(resourceBuilder, properties.get(HEADERS));
        return resourceBuilder;
    }

    private ClientResponse getResponse(WebResource.Builder webResource) {
        ClientResponse response;
        if ("GET".equals(properties.get(METHOD))) {
            response = webResource.get(ClientResponse.class);
        } else {
            //TODO pending review POST request implementation
            response = webResource.post(ClientResponse.class);
        }

        return response;
    }

    /**
     * Convert Multivalued Headers to Plain Map Headers accepted by Flume Event.
     *
     * @param map multivaluedMap.
     * @return plain Map.
     */
    private Map<String, String> responseToHeaders(MultivaluedMap<String, String> map) {
        Map<String, String> newMap = new HashMap<String, String>();
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            newMap.put(entry.getKey(), multiValueHeaderToString(entry.getValue()));
        }
        return newMap;
    }

    /**
     * Convert a multivalue header to String.
     *
     * @param list
     * @return
     * @see <a href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2">w3.org</a>
     */
    private String multiValueHeaderToString(List<String> list) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); ++i) {
            sb.append(list.get(i));
            if (i != list.size() - 1) {
                sb.append(", ");
            }
        }

        return sb.toString();
    }

    /**
     * Initialize properties that are received in the {@code SchedulerContext}.
     *
     * @param context
     */
    @SuppressWarnings("unchecked")
    public void initProperties(SchedulerContext context) {
        queue = (LinkedBlockingQueue<Event>) context.get("queue");
        properties = (Map<String, String>) context.get("properties");
        client = (Client) context.get("client");
        handler = (RestSourceHandler)context.get("handler");

    }

    private RestSourceHandler getHandler(SchedulerContext context) {
        RestSourceHandler handler = null;
        try {
            handler = (RestSourceHandler) Class.forName((String) context.get(HANDLER)).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return handler;
    }

    /**
     * Set an Application Type to the request depending on a parameter and its corresponding
     * {@code MediaType}.
     *
     * @param webResource     Current target url.
     * @param applicationType ApplicationType to set.
     * @return
     */
    public WebResource.Builder setApplicationType(WebResource webResource, String applicationType) {
        if ("TEXT".equals(applicationType)) {
            mediaType = MediaType.TEXT_PLAIN_TYPE;
        } else {
            mediaType = MediaType.APPLICATION_JSON_TYPE;
        }

        return webResource.accept(mediaType);
    }

    /**
     * Map raw Json to an object and add each key-value to a headers request.
     *
     * @param builder     Current REST request.
     * @param jsonHeaders raw json.
     * @return
     */
    private WebResource.Builder addHeaders(WebResource.Builder builder, String jsonHeaders) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            Map<String, Object> headers = mapper.readValue(jsonHeaders, Map.class);
            for (Map.Entry<String, Object> entry : headers.entrySet()) {
                builder.header(entry.getKey(), entry.getValue());
            }
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return builder;
    }
}
