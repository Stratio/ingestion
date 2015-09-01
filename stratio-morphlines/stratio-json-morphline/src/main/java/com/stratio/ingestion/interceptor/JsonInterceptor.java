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
package com.stratio.ingestion.interceptor;

import static com.stratio.ingestion.interceptor.Constants.LINES;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class JsonInterceptor implements Interceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonInterceptor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public void initialize() {

    }

    public Event intercept(Event event) {
        return null;
    }

    public List<Event> intercept(List<Event> events) {

        List<Event> resultList = new ArrayList<>();

        for (Event ev : events) {
            List<Event> partialEventsList = new ArrayList<>();
            Map<String, String> headers = ev.getHeaders();

            if (headers.containsKey(LINES)) {
                String linesFieldValue = headers.get(LINES);
                JsonNode rootNode = null;
                Map<String, String> headersTmp = Maps.newHashMap(headers);
                headersTmp.remove(LINES);

                try {
                    rootNode = objectMapper.readTree("{\"" + LINES + "\":" + linesFieldValue + "}");
                } catch (IOException e) {
                    LOGGER.error("Error parsing \"lines\" fields in JSON", e);
                }
                Iterator<JsonNode> products = rootNode.get(LINES).getElements();
                while (products.hasNext()) {
                    JsonNode product = products.next();
                    Iterator<Map.Entry<String, JsonNode>> productFields = product.getFields();
                    while (productFields.hasNext()) {
                        Map.Entry<String, JsonNode> next = productFields.next();
                        headersTmp.put(next.getKey(), next.getValue().getValueAsText());
                    }
                    partialEventsList.add(EventBuilder.withBody("", StandardCharsets.UTF_8, headersTmp));
                }
            } else {
                partialEventsList.add(ev);
            }
            resultList.addAll(partialEventsList);
        }
        return resultList;
    }

    public void close() {

    }

    /**
     * Builder which builds new instance of the StaticInterceptor.
     */
    public static class Builder implements Interceptor.Builder {

        @Override
        public void configure(Context context) {
        }

        @Override
        public Interceptor build() {
            LOGGER.info("Creating StaticInterceptor -> JsonInterceptor.");
            return new JsonInterceptor();
        }
    }

}


