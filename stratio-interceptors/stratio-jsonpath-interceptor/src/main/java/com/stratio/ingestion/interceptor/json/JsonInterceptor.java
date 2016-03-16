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
package com.stratio.ingestion.interceptor.json;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.jayway.jsonpath.JsonPath;

//@formatter:off
/**
* <p>JsonPath Interceptor. Read headers or body event as Json and compile a JsonPathExpression to create one
 * event for each element result of apply that expression to the json in headers or body.
 * Maintain whole json in body is optional.</p>.
* <ul>
* <li><em>jsonHeader</em>: Header used to read the JSON content. Optinal, if not specified we will use the body.</li>
* <li><em>expression</em>: Jsonpath expression. </li>
* <li><em>overwriteBody</em>: Overwrite event body with a json representation of headers
 * Default: empty
* </ul>
*/
//@formatter:on
public class JsonInterceptor implements Interceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonInterceptor.class);

    private String expression;
    private String inputHeader;
    private Boolean overwriteBody;

    private JsonInterceptor(String expression, String headerName, Boolean overwriteBody) {
        this.expression= expression;
        this.inputHeader= headerName;
        this.overwriteBody= overwriteBody;
    }

    @Override
    public void initialize() {
    }

    @Override
    public void close() {
    }

    public Event intercept(Event event) {
        return event;
    }


    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> generatedEvents= new ArrayList<>();
        for (Event event : events) {
            generatedEvents= deserializeEvent(event);
        }
        return generatedEvents;
    }


    public List<Event> deserializeEvent(Event event) {
        Map<String, String> headers = event.getHeaders();
        String body= new String(event.getBody());
        List<Event> events= new ArrayList<>();

        if (null != inputHeader && !inputHeader.isEmpty() && headers.containsKey(inputHeader)) {
            events = parseJsonEvents(headers.get(inputHeader), headers, body);

        } else if (null != body && !body.isEmpty()) {
            events = parseJsonEvents(body, headers, body);

        } else {
            LOGGER.debug("Event doesn't includes " + inputHeader + " header.");
        }

        return events;
    }

    public List<Event> parseJsonEvents(String jsonToParse,  Map<String, String> headers, String body) {

        List<Event> events= new ArrayList<>();
        Map<String, String> copyOfHeaders = new HashMap<>();

        try {
            List<Map<String, Object>> elements = JsonPath.read(jsonToParse, expression);

            for (Map<String, Object> node : elements) {
                copyOfHeaders = Maps.newHashMap(headers);

                Set<String> keys = node.keySet();
                for (String key : keys) {
                    copyOfHeaders.put(key, node.get(key).toString());
                }

                if (overwriteBody) {
                    body = createJsonFromHeaders(headers);
                    LOGGER.debug("#Body Events: " + body);
                }
                events.add(EventBuilder.withBody(body, StandardCharsets.UTF_8, copyOfHeaders));
            }
        } catch (Exception ex) {
            LOGGER.debug("Unable to parse json found in header: " + ex.getMessage());
        }
        return events;
    }



    /**
     * Builder which builds new instance of the StaticInterceptor.
     */
    public static class Builder implements Interceptor.Builder {

        private static final String CONF_EXPRESSION = "expression";
        private static final String CONF_INPUT_HEADER = "jsonHeader";
        private static final String CONF_OVERWRITE_BODY = "overwriteBody";
        private String expression;
        private String header;
        private Boolean overwriteBody= false;

        @Override
        public void configure(Context context) {
            expression = context.getString(CONF_EXPRESSION);
            Preconditions.checkArgument(!StringUtils.isEmpty(expression),
                    "Must supply a valid expression used to deserialize the json. " +
                            "So you need to specify the " + CONF_EXPRESSION + " attribute.");

            header = context.getString(CONF_INPUT_HEADER);

            overwriteBody= context.getBoolean(CONF_OVERWRITE_BODY);
            if (overwriteBody == null)
                overwriteBody= false;
        }

        @Override
        public Interceptor build() {
            LOGGER.debug("Creating StaticInterceptor -> JsonInterceptor");
            return new JsonInterceptor(expression, header, overwriteBody);
        }
    }

    private String createJsonFromHeaders(Map<String, String> headers) {

        Map<String, Object> mapTyped = new HashMap<>();

        for (String key : headers.keySet()) {
            String value = headers.get(key);
            Object obj = value;
            try {
                obj = Long.valueOf(value);
            } catch (NumberFormatException nfe) {

            }
            try {
                obj = Double.valueOf(value);
            } catch (NumberFormatException nfe) {

            }
            mapTyped.put(key,obj);
        }

        try {
            return new ObjectMapper().writeValueAsString(mapTyped);
        } catch (IOException e) {
            LOGGER.error("Error to generate json from headers");
        }
        return "";
    }

}
