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

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.agent.embedded.EmbeddedAgent;
import org.apache.flume.event.EventBuilder;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Created by anavidad on 1/09/15.
 */
public class JsonInterceptorTest {

    @Test
    public void testJsonInterceptorTest() throws EventDeliveryException, FlumeException {

        Map<String, String> properties = new HashMap<>();
        properties.put("channel.type", "memory");
        properties.put("channel.capacity", "200");
        properties.put("sinks", "sink1");
        properties.put("sink1.type", "avro");
        properties.put("sink1.hostname", "localhost");
        properties.put("sink1.port", "5564");
        properties.put("processor.type", "default");
        properties.put("source.interceptors", "i1");
        properties.put("source.interceptors.i1.type", "com.stratio.ingestion.interceptor.JsonInterceptor$Builder");

        EmbeddedAgent agent = new EmbeddedAgent("myagent");

        agent.configure(properties);
        agent.start();
        agent.putAll(buildEvents());

        //TODO Assert conditions
        agent.stop();
    }

    private List<Event> buildEvents() {

        List<Event> events = Lists.newArrayList();
        ObjectMapper objectMapper = new ObjectMapper();

        try {

            JsonNode rootNode = objectMapper.readTree(
                    "{\"order_id\": \"00000000-0000-0000-C000-000000000046\", \"timestamp\": \"2014-12-08T11:22:45 -01:00\",\"client_id\": 7, \"latitude\": 5.34644, \"longitude\": -74.49147, \"payment_method\": \"credit card\", \"credit_card\": \"4444 3333 2222 1111\", \"shopping_center\": \"Barcelona\", \"employee\": 48, \"total_amount\": 2880.93,    \"lines\": [      {        \"product\": \"PEANUTS\",        \"family\": \"Feeding\",        \"quantity\": 3,        \"price\": 46.03      },      {        \"product\": \"PIZZA\",        \"family\": \"Feeding\",        \"quantity\": 5,        \"price\": 23.64      },      {        \"product\": \"SOUP\",        \"family\": \"Feeding\",        \"quantity\": 5,        \"price\": 48.59      }    ]  }");

            Map<String, String> headers = new HashMap<>();
            headers.put(Constants.ORDER_ID, rootNode.get(Constants.ORDER_ID).getTextValue());
            headers.put(Constants.TIMESTAMP, rootNode.get(Constants.TIMESTAMP).getValueAsText());
            headers.put(Constants.LONGITUDE, rootNode.get(Constants.LONGITUDE).getValueAsText());
            headers.put(Constants.LINES, rootNode.get(Constants.LINES).toString());

            Map<String, String> headers2 = new HashMap<>();
            headers2.put(Constants.ORDER_ID, rootNode.get(Constants.ORDER_ID).getTextValue());
            headers2.put(Constants.TIMESTAMP, rootNode.get(Constants.TIMESTAMP).getValueAsText());
            headers2.put(Constants.LONGITUDE, rootNode.get(Constants.LONGITUDE).getValueAsText());

            Event event = EventBuilder.withBody("body", Charset.defaultCharset(), headers);
            events.add(event);
            Event event2 = EventBuilder.withBody("body", Charset.defaultCharset(), headers2);
            events.add(event2);

            return events;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return events;
    }
}
