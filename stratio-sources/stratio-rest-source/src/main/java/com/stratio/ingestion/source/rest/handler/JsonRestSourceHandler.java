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

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.base.Charsets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.stratio.ingestion.source.rest.exception.RestSourceException;

/**
 * Created by eambrosio on 13/01/15.
 */
public class JsonRestSourceHandler implements RestSourceHandler {

    protected static final String DEFAULT_JSON_PATH = "";
    protected static final String CONF_PATH = "jsonPath";
    private String path;
    private Gson gson;
    private Type listType = new TypeToken<List<Event>>() {
    }.getType();

    public JsonRestSourceHandler() {
        this.gson = new GsonBuilder().disableHtmlEscaping().create();
    }

    @Override public List<Event> getEvents(String body, Map<String, String> headers) {

        List<Event> events = new ArrayList<Event>(0);
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode jsonNode = mapper.readTree(body);
            if (jsonNode.isObject()) {
                    events.add(buildSingleEvent(headers, findValue(jsonNode,path)));
            }
            if (jsonNode.isArray()) {
                final Iterator<JsonNode> elements = jsonNode.getElements();
                JsonNode element;
                while (elements.hasNext()) {
                    element = elements.next();
                    events.add(buildSingleEvent(headers, findValue(element,path)));
                }
            }

        } catch (Exception e) {
            throw new RestSourceException("An error ocurred while response parsing", e);
        }

        return events;

    }

    private JsonNode findValue(JsonNode jsonNode, String path) {
        JsonNode node = jsonNode;
        if (!DEFAULT_JSON_PATH.equals(path)){
            node = jsonNode.findValue(path);
        }
        return node;
    }

    private Event buildSingleEvent(Map<String, String> headers, JsonNode jsonNode) {
        return EventBuilder.withBody(jsonNode.toString(), Charsets.UTF_8, headers);
    }

    @Override public void configure(Context context) {
        path = context.getString(CONF_PATH, DEFAULT_JSON_PATH);
    }
}
