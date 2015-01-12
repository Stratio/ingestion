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
package com.stratio.ingestion.source.batch.rest;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import javax.ws.rs.core.MediaType;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.http.HttpStatus;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * RequestJob. Quartz Job that make a request to a RESTful service.
 */
public class RequestJob extends AbstractJob implements Job {

    private static final Logger log = LoggerFactory.getLogger(RequestJob.class);

    public static final String APPLICATION_TYPE = "applicationType";
    public static final String METHOD = "method";
    public static final String URL = "url";
    public static final String HEADERS = "headers";

    private Map<String, String> properties;
    private LinkedBlockingQueue<Event> queue;
    private Client client;
    private MediaType mediaType;
    private JobExecutionContext context;

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
            addEventsToQueue(response);

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

    protected void addEventsToQueue(ClientResponse response) {
        if (response.getStatus() == HttpStatus.SC_OK) {
            String responseString = response.getEntity(String.class);
            ObjectMapper mapper = new ObjectMapper();
            try {
                JsonNode jsonNode = mapper.readTree(responseString);
                final Iterator<JsonNode> elements = jsonNode.getElements();
                JsonNode element = null;
                while (elements.hasNext()) {
                    element = elements.next();
                    if (isValid(element, properties.get(CONF_CHECKPOINT_TYPE), properties.get
                            (CONF_CHECKPOINT_VALUE), properties.get(CONF_CHECKPOINT_FIELD), properties)) {
                        queue.add(EventBuilder.withBody(element.toString(), Charsets.UTF_8,
                                responseToHeaders(response.getHeaders())));
                    }
                }
                addCheckpoint(response, mapper, element);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void addCheckpoint(ClientResponse response, ObjectMapper mapper, JsonNode element) {
        if (isCheckpointEnabled(properties.get(CONF_CHECKPOINT_ENABLED)) && isValid(element,
                properties.get(CONF_CHECKPOINT_TYPE), properties.get(CONF_CHECKPOINT_VALUE),
                properties.get(CONF_CHECKPOINT_FIELD), properties)) {
            queue.add(EventBuilder.withBody(populateCheckpoint(mapper, element).toString(), Charsets.UTF_8,
                    responseToHeaders(response.getHeaders())));
            updateLastEventProperty(element, properties, properties.get(CONF_CHECKPOINT_FIELD));
        }
    }

    private ClientResponse getResponse(WebResource.Builder request) {
        ClientResponse response = null;
        if ("GET".equals(properties.get(METHOD))) {
            response = request.get(ClientResponse.class);
        } else {
            //TODO pending review POST request
            response = request.post(ClientResponse.class);
        }

        return response;
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
        WebResource.Builder builder;
        if ("TEXT".equals(applicationType)) {
            mediaType = MediaType.TEXT_PLAIN_TYPE;
        } else {
            mediaType = MediaType.APPLICATION_JSON_TYPE;
        }
        builder = webResource.accept(mediaType);

        return builder;
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
