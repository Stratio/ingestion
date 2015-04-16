/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.ingestion.sink.druid;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.flume.Event;

/**
 * Created by eambrosio on 15/04/15.
 */
public class EventParser {

    private final String timestampField;

    public EventParser(String timestampField) {
        this.timestampField = timestampField;
    }

    protected List<Map<String, Object>> parse(List<Event> events) {
        List<Map<String, Object>> parsedEvents = new ArrayList<Map<String, Object>>();
        if (!CollectionUtils.isEmpty(events)) {
            for (Event event : events) {
                parsedEvents.add(parseEvent(event));
            }
        }
        return parsedEvents;
    }

    private Map<String, Object> parseEvent(Event event) {
        final Map<String, String> headers = event.getHeaders();
        Map<String, Object> parsedEvent = null;
        Random random = new Random();
        if (MapUtils.isNotEmpty(headers)) {
            parsedEvent = new HashMap<String, Object>();
            for (String header : headers.keySet()) {
                if (timestampField.equalsIgnoreCase(header)) {
                    parsedEvent.put(header, Long.valueOf(headers.get(header)));
                    //                    try {
                    //                        Thread.sleep(1000);
                    //                    } catch (InterruptedException e) {
                    //                        e.printStackTrace();
                    //                    }
                } else {
                    parsedEvent.put(header, headers.get(header));
                }
                //            parsedEvent.put(header, headers.get(header));

            }
        }
        return parsedEvent;
    }

}

