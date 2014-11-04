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
package com.stratio.ingestion.deserializer.xmlxpath;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
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

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.Seekable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

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
public class XmlXpathDeserializer implements EventDeserializer {

    private static final Logger log = LoggerFactory.getLogger(XmlXpathDeserializer.class);

    private static final String CONF_XPATH_EXPRESSION = "expression";
    private static final String CONF_ELEMENT = "outputField";
    private static final String CONF_HEADERS = "headers.";

    private static final String DEFAULT_ELEMENT_FIELD = "element";

    private boolean isOpen;
    private String elementField;
    private String body;
    private Map<String, String> staticHeaders;
    private final XPath xpath;
    private Document doc = null;
    private List<String> list = null;
    private ListIterator<String> markIt, currentIt;

    XmlXpathDeserializer(Context context, InputStream seekableInputStream)
            throws IOException {

        final String expression = context.getString(CONF_XPATH_EXPRESSION);
        elementField = context.getString(CONF_ELEMENT, DEFAULT_ELEMENT_FIELD);
        final ImmutableMap<String, String> headers = context.getSubProperties(CONF_HEADERS);
        final InputStream inStream = seekableInputStream;

        xpath = XPathFactory.newInstance().newXPath();
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder;
        try {
            docBuilder = factory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            throw new IOException("Creating DocumentBuilder failed", e);
        }

        try {
            doc = docBuilder.parse(inStream);
        } catch (SAXException e) {
            throw new IOException("Cannot parse body", e);
        }

        try {
          inStream.close();
        } catch (IOException ex) {
          log.warn("Error while closing input stream");
        }

        // Extract full xml to body
        try {
            body = documentToString(doc);
        } catch (TransformerException e) {
          throw new IOException("Cannot serialize XML", e);
        }

        // Add static fiels. These will be included in all events.
        staticHeaders = evaluateStaticFields(headers);

        if (doc != null) {
            isOpen = true;
        }

        NodeList nodeList;
        try {
            final XPathExpression expr = xpath.compile(expression);
            nodeList = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
            list = new ArrayList<String>(nodeList.getLength());
            log.debug("XPath expression matched {} elements", list.size());
        } catch (XPathExpressionException e) {
            throw new IOException("Applying XPath expression failed", e);
        }

        for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);
            String eventSt = nodeToString(node);
            list.add(eventSt);
        }

        markIt = list.listIterator();
        currentIt = list.listIterator();
    }

    @Override
    public Event readEvent() throws IOException {
        ensureOpen();
        if (!currentIt.hasNext()) {
            return null;
        } else {
            String event = currentIt.next();
            Event ev = EventBuilder.withBody(body, Charsets.UTF_8);
            ev.getHeaders().put(elementField, event);
            ev.getHeaders().putAll(staticHeaders);
            return ev;
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
        markIt = index >= 0 ? list.listIterator(currentIt.previousIndex()) : list.listIterator(0);
        if (markIt.hasNext()) {
            markIt.next();
        }
    }

    @Override
    public void reset() throws IOException {
        ensureOpen();
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
        public EventDeserializer build(Context context, InputStream in) {
            if (!(in instanceof Seekable)) {
                throw new IllegalArgumentException(
                        "Cannot use this deserializer without a Seekable input stream");
            }
            try {
                return new XmlXpathDeserializer(context, in);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

}
