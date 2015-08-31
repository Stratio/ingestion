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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.avro.data.Json;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableInputStream;
import org.apache.flume.serialization.Seekable;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.impl.JsonReadContext;
import org.codehaus.jackson.map.deser.JsonNodeDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.internal.JsonReader;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.JSONParser;

//@formatter:off
/**
* <p>JSON XPath Deserializer. Read InputStream as JSON compile a XPathExpression and create event for each element
* result of apply that expression to the json in headers. Maintain whole json in body.</p>.
* <ul>
* <li><em>outputField</em>: Output Field in header where put events. Default: element.</li>
* <li><em>expression</em>: XPath expression. </li>
* </ul>
* 
* <p>A special option is the chance to evaluate xpath expression for each event and add result in a header. For example:</p>
* <code>
* <li>headers.author= <JsonXPathExpression> will put result of expression in author field of header.</li>
* </code>
*/
//@formatter:on
public class JsonXpathDeserializer implements EventDeserializer {

    private static final Logger log = LoggerFactory.getLogger(JsonXpathDeserializer.class);

    private static final String CONF_XPATH_EXPRESSION = "expression";
    private static final String CONF_OUTPUT_HEADER = "outputHeader";
    private static final String CONF_OUTPUT_BODY = "outputBody";

    private static final boolean DEFAULT_OUTPUT_BODY = true;

    private boolean isOpen;
    private String outputHeader;
    private boolean outputBody;
    private String body;
    private final XPath xpath = XPathFactory.newInstance().newXPath();
//    private final JsonPath jsonpath = new JsonPath();
    private Document doc = null;
    private List<String> list = null;
    private ListIterator<String> markIt, currentIt;

    JSONObject object;
    ArrayList<String> completeList = new ArrayList<>();

    JsonXpathDeserializer(Context context, ResettableInputStream in) throws IOException {
        try {

            final String expression = context.getString(CONF_XPATH_EXPRESSION);
            outputBody = context.getBoolean(CONF_OUTPUT_BODY, DEFAULT_OUTPUT_BODY);
            if (!outputBody) {
                if (!context.containsKey(CONF_OUTPUT_HEADER)) {
                    throw new ConfigurationException(
                            String.format("Either %s must be false or %s must be defined", CONF_OUTPUT_BODY, CONF_OUTPUT_HEADER));
                }
                outputHeader = context.getString(CONF_OUTPUT_HEADER);
            }


            InputStream jsonStream = new ResettableInputStreamInputStream(in);

            //Json doesn't exists
            try {
                object = JSONValue.parse(jsonStream, JSONObject.class);
            } catch (Exception ex) {
                throw new IOException("JSON doesn't exists", ex);
            }

            //Json is not empty
            try {
                body = object.toString();
            } catch (Exception ex) {
                throw new IOException("JSON is empty", ex);
            }

            if(body != null) {
                isOpen = true;
            }

            //Lines has products
            try {
                String lines = String.valueOf(object.get("lines"));
                String[] arrayLines = lines.split("},");
                Integer numLines = arrayLines.length;

                if (numLines <= 2) {
                    log.debug("Json has no products");
                }
                System.out.println(arrayLines[1]);

            } catch (Exception ex) {
                throw new IOException("Lines is empty", ex);
            }

            //Separate products and headers
            try {
                String lines = String.valueOf(object);

                String regex = "\\[";
                String[] arrayLines = lines.split(regex);
                String[] arrayHeaders1 = arrayLines[0].split(",");
                String[] arrayHeaders2 = arrayLines[1].split("],");

                ArrayList<String> listaCabeceras = new ArrayList<>();
                for (int i = 0; i < arrayHeaders1.length -1; i++) {
                    listaCabeceras.add(arrayHeaders1[i]);
                }

                for (int i = 0; i < arrayHeaders2.length; i++) {
                    if(i == 1){
                        listaCabeceras.add(arrayHeaders2[i]);
                    }
                }

                String[] productList = arrayHeaders2[0].split("}");



                for (int i = 0; i < productList.length; i++) {
                    completeList.add(productList[i] + listaCabeceras);
                }

//                System.out.println("--------***************-------------");
                markIt = completeList.listIterator();
                currentIt = completeList.listIterator();

//                while (markIt.hasNext()) {
//                    System.out.println("--------***************-------------");
//                    System.out.println(markIt.next());
//
//                }
//                markIt = list.listIterator();
//                currentIt = list.listIterator();

            } catch (Exception ex) {
                throw new IOException("Product list format failed", ex);
            }

        }   finally {
              try {
                in.close();
              } catch (IOException ex) {
                log.warn("Error while closing input stream");
              }
        }
    }

    @Override
    public Event readEvent() throws IOException {
        ensureOpen();
        if (!currentIt.hasNext()) {
            return null;
        } else {
            final String node = currentIt.next();
//            System.out.println(node);
//            System.out.println(outputBody);
            if (outputBody) {
              return EventBuilder.withBody(node, Charsets.UTF_8);
            } else {
              final Event event = EventBuilder.withBody(body, Charsets.UTF_8);
              event.getHeaders().put(outputHeader, node);
              return event;
            }
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

    @Override
    public void mark() throws IOException {
        ensureOpen();
        int index = currentIt.previousIndex();
        markIt = index >= 0 ? completeList.listIterator(currentIt.previousIndex()) : completeList.listIterator(0);
        if (markIt.hasNext()) {
            markIt.next();
        }
    }

    @Override
    public void reset() throws IOException {
        ensureOpen();
        int index = markIt.previousIndex();
        currentIt = index >= 0 ? completeList.listIterator(markIt.previousIndex()) : completeList.listIterator(0);
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

    private void ensureOpen() {
        if (!isOpen) {
            throw new IllegalStateException("Serializer has been closed");
        }
    }

    public String nodeToString(Node node) {
        StringWriter writer = new StringWriter();
        TransformerFactory tfactory = TransformerFactory.newInstance();
        Transformer xform;
        try {
            xform = tfactory.newTransformer();
            xform.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            Source src = new DOMSource(node);
            Result result = new StreamResult(writer);
            xform.transform(src, result);
        } catch (TransformerException e) {
            e.printStackTrace();
        }
        return writer.toString();
    }

    public String documentToString(Document document) throws TransformerException {
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer = tf.newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        StringWriter writer = new StringWriter();
        transformer.transform(new DOMSource(document), new StreamResult(writer));
        return writer.getBuffer().toString().replaceAll("\n|\r|\t", "");
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
                XPathExpression expression = xpath.compile(entry.getValue());
                String value = (String) expression.evaluate(doc, XPathConstants.STRING);
                headers.put(entry.getKey(), value);
            } catch (XPathExpressionException e) {
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
                return new JsonXpathDeserializer(context, in);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

}
