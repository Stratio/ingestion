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
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.event.JSONEvent;

import com.google.common.base.Charsets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Created by eambrosio on 13/01/15.
 */
public class DefaultRestSourceHandler implements RestSourceHandler {

    private Gson gson;
    private final Type listType = new TypeToken<List<JSONEvent>>() {
    }.getType();

    public DefaultRestSourceHandler() {
        this.gson = new GsonBuilder().disableHtmlEscaping().create();
    }

    @Override public List<Event> getEvents(String body, Map<String, String> headers) {
        List<Event> events = new ArrayList<>(0);
        events.add(EventBuilder.withBody(body, Charsets.UTF_8, headers));

        return events;

    }

    @Override public void configure(Context context) {

    }
}
