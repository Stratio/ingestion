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
package com.stratio.ingestion.deserializer.jsonxpath;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableInputStream;
import org.apache.flume.serialization.Seekable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.transform.TransformerException;
import net.minidev.json.JSONArray;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

//@formatter:off
/**
 * <p>XML XPath Deserializer. Read InputStream as XML compile a XPathExpression and create event for each element
 * result of apply that expression to the xml in headers. Maintain whole xml in body.</p>.
 * <ul>
 * <li><em>outputField</em>: Output Field in header where put events. Default: element.</li>
 * <li><em>expression</em>: XPath expression. </li>
 * </ul>
 *
 * <p>A special option is the chance to evaluate xpath expression for each event and add result in a header. For example:</p>
 * <code>
 * <li>headers.author= <XPathExpression> will put result of expression in author field of header.</li>
 * </code>
 */
//@formatter:on
public class JsonPathDeserializer implements EventDeserializer {

    private static final Logger log = LoggerFactory.getLogger(JsonPathDeserializer.class);

    private static final String CONF_XPATH_EXPRESSION = "expression";
    private static final String CONF_OUTPUT_HEADER = "outputHeader";
    private static final String CONF_OUTPUT_BODY = "outputBody";
    private static final String BODY_EXPRESSION = "$.";

    private String expression;
    private static final boolean DEFAULT_OUTPUT_BODY = true;

    private boolean flagMark= false;
    private boolean flagReset= false;
    private boolean isOpen;
    private String outputHeader;
    private boolean outputBody;
    private ReadContext ctx;
    private String body;
    private List list = null;
    private ListIterator markIt, currentIt;

    JsonPathDeserializer(Context context, ResettableInputStream in) throws IOException {
        try {
            expression = context.getString(CONF_XPATH_EXPRESSION);
            outputBody = context.getBoolean(CONF_OUTPUT_BODY, DEFAULT_OUTPUT_BODY);
            if (!outputBody) {
                if (!context.containsKey(CONF_OUTPUT_HEADER)) {
                    throw new ConfigurationException(
                            String.format("Either %s must be false or %s must be defined", CONF_OUTPUT_BODY, CONF_OUTPUT_HEADER));
                }
                outputHeader = context.getString(CONF_OUTPUT_HEADER);
            }

            ctx = JsonPath.parse(new ResettableInputStreamInputStream(in));

            if (ctx != null) {
                isOpen = true;
            }

            body = ctx.jsonString();
            list = new ArrayList<Object>();
            
            Object result = JsonPath.read(body, expression);

            if (result instanceof String)  {
                String item = (String) result;
                list.add(item);
            }   else {
            //String item = JsonPath.read(body, BODY_EXPRESSION);
                JSONArray nodes= (JSONArray) result;
                Iterator itr= nodes.iterator();
                while (itr.hasNext())   {
                    list.add(itr.next());
                    //LinkedHashMap node= (java.util.LinkedHashMap) itr.next();
                    //node.values().
                    //list.add(node);
                }
                
            }
            markIt = list.listIterator();
            currentIt = list.listIterator();
            
            /**
            list = ctx.read(expression);

            markIt = list.listIterator();
            currentIt = list.listIterator();
            */
        } catch (Exception e) {
           throw new IOException("Cannot serialize JSON", e);

        } finally {
            try {
                in.close();
            } catch (IOException ex) {
                log.warn("Error while closing input stream");
            }
        }
    }

    public Event composeEventWithHeaders(String node)    {
        Event event= EventBuilder.withBody(node, Charsets.UTF_8);
        
        //JSONObject.fromObject(node);
        try {
            if (node.isEmpty())
                return event;
            JSONObject jObject= new JSONObject(node);
            
            Iterator<?> keys = jObject.keys();

            while( keys.hasNext() ) {
                String key = (String)keys.next();
                if ( jObject.get(key) instanceof JSONObject ) {

                }   else if ( jObject.get(key) instanceof String ) {
                    event.getHeaders().put(key, jObject.get(key).toString());
                }   else if ( jObject.get(key) instanceof Integer ) {
                    event.getHeaders().put(key, jObject.get(key).toString());
                }   else if ( jObject.get(key) instanceof Double ) {
                    event.getHeaders().put(key, jObject.get(key).toString());
                } 
            }            
            
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
                
        return event;
    }
    
    @Override
    public Event readEvent() throws IOException {
        ensureOpen();
        
        if (!currentIt.hasNext()) {
            return null;
        } else {
            Object result = currentIt.next();
            String node = "";
            Event event= composeEventWithHeaders(body);
            
            if(result instanceof String) {
                node = (String)result;
                
                event.getHeaders().put(outputHeader, node);
                
            }   else if (result instanceof java.util.LinkedHashMap)  {
                java.util.LinkedHashMap elements= (java.util.LinkedHashMap) result;
                int size= ((java.util.LinkedHashMap) result).size();
                Iterator itr= ((java.util.LinkedHashMap) result).keySet().iterator();
                while (itr.hasNext())   {
                    String key= (String) itr.next();
                     
                    event.getHeaders().put(key, ((java.util.LinkedHashMap) result).get(key).toString());
                    //System.out.println("Element " + key + " - " + ((java.util.LinkedHashMap) result).get(key));
                    
                }

            }
            return event;
        }
        
    }

    @Override
    public List<Event> readEvents(int numEvents) throws IOException {
        ensureOpen();
        List<Event> events = Lists.newLinkedList();
        for (int i = 0; i < numEvents; i++) {
            Event event = readEvent();
            if (event != null) {
                events.add(event);
            }
        }
        return events;
    }
    
    public List<Event> readEvents() throws IOException {
        return readEvents(list.size());
    }

    @Override
    public void mark() throws IOException {
        ensureOpen();
        flagMark= true;
        
        int index = currentIt.previousIndex();
        markIt = index >= 0 ? list.listIterator(currentIt.previousIndex()) : list.listIterator(0);
        if (markIt.hasNext()) {
            markIt.next();
        }
        
    }

    @Override
    public void reset() throws IOException {
        ensureOpen();
        flagReset= true;
        
        int index = markIt.previousIndex();
        currentIt = index >= 0 ? list.listIterator(markIt.previousIndex()) : list.listIterator(0);
        if (currentIt.hasNext()) {
            currentIt.next();
        }
        
    }

    @Override
    public void close() throws IOException {
        if (isOpen) {
            isOpen = false;
        }
    }

    public String documentToString() throws TransformerException {
        return ctx.jsonString();
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw new IllegalStateException("Serializer has been closed");
        }
    }

    /**
     * From a properties, evaluate every xpath expression in value and put result in a map
     * maintaining given key.
     * 
     * @param properties
     * @return
     */
    private Map<String, String> evaluateStaticFields(ImmutableMap<String, String> properties) {
        Map<String, String> headers = new HashMap<String, String>();
        for (Entry<String, String> entry : properties.entrySet()) {
            try {
                String value = JsonPath.read(body, expression);
                headers.put(entry.getKey(), value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return headers;
    }    
    
    
    public static class Builder implements EventDeserializer.Builder {

        @Override
        public EventDeserializer build(Context context, ResettableInputStream in) {
            if (!(in instanceof Seekable)) {
                throw new IllegalArgumentException(
                        "Cannot use this deserializer without a Seekable input stream");
            }
            try {
                return new JsonPathDeserializer(context, in);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

}
