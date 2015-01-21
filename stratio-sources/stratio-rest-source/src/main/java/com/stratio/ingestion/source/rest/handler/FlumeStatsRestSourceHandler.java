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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.JSONEvent;
import org.codehaus.jackson.map.ObjectMapper;

import com.stratio.ingestion.source.rest.exception.RestSourceException;

public class FlumeStatsRestSourceHandler implements RestSourceHandler {

    @Override public void configure(Context context) {

    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Event> getEvents(final String body, final Map<String, String> httpHeaders) {
        final List<Event> events = new ArrayList<Event>(0);
        try {
            ObjectMapper mapper = new ObjectMapper();
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>)mapper.readValue(body, Map.class);

            for (Map.Entry<String, Object> entry : map.entrySet()) {
                final Map<String, String> headers = new HashMap<String, String>();
                final String key = entry.getKey();
                final String name = key.substring(key.indexOf(".") + 1, key.length());
                headers.put("Name", name);
                for (Map.Entry<String, Object> componentInfo : ((Map<String,Object>)entry.getValue()).entrySet()) {
                    headers.put(componentInfo.getKey(), componentInfo.getValue().toString());
                }
                final Event event = new JSONEvent();
                event.setHeaders(headers);
                event.setBody("".getBytes());
                events.add(event);
            }
        } catch (IOException ex) {
            throw new RestSourceException(ex);
        } catch (ClassCastException ex) {
            throw new RestSourceException(ex);
        }

        return events;

    }

}
