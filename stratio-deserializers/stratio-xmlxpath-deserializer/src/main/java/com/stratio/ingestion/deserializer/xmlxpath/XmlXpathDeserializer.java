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
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

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
import org.apache.flume.serialization.ResettableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

public class XmlXpathDeserializer implements EventDeserializer {

    private static final Logger log = LoggerFactory.getLogger(XmlXpathDeserializer.class);

    private static final String CONF_XPATH_EXPRESSION = "expression";

    private boolean isOpen;
    private String expression;
    private final XPath xpath;
    private XPathExpression expr;
    private DocumentBuilder docBuilder;
    private Document doc = null;
    private ResettableInputStream resIn;
    private final ResettableInputStreamInputStream inStream;
    private List<String> list = null;
    private NodeList nodeList;
    private ListIterator<String> markIt, currentIt;

    XmlXpathDeserializer(Context context, ResettableInputStream resettableInputStream)
            throws IOException {

        expression = context.getString(CONF_XPATH_EXPRESSION);
        resIn = resettableInputStream;
        inStream = new ResettableInputStreamInputStream(resettableInputStream);

        xpath = XPathFactory.newInstance().newXPath();
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try {
            docBuilder = factory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }

        try {
            doc = docBuilder.parse(inStream);
        } catch (SAXException e) {
            log.error("Cannot parse body");
            e.printStackTrace();
        }

        if (doc != null) {
            isOpen = true;
        }

        try {
            expr = xpath.compile(expression);
            nodeList = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
            list = new ArrayList<String>(nodeList.getLength());
        } catch (XPathExpressionException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < nodeList.getLength(); i++) {
            Element el = (Element) nodeList.item(i);
            String eventSt = element2String(el);
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
            return EventBuilder.withBody(event, Charsets.UTF_8);
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
        if(markIt.hasNext()){
            markIt.next();
        }
    }

    @Override
    public void reset() throws IOException {
        ensureOpen();
        int index = markIt.previousIndex();
        currentIt = index >= 0 ? list.listIterator(markIt.previousIndex()) : list.listIterator(0);
        if(currentIt.hasNext()){
            currentIt.next();
        }
    }

    @Override
    public void close() throws IOException {
        if (isOpen) {
            reset();
            inStream.close();
            isOpen = false;
        }
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw new IllegalStateException("Serializer has been closed");
        }
    }

    public String element2String(Element el) {
        StringWriter writer = new StringWriter();
        TransformerFactory tfactory = TransformerFactory.newInstance();
        Transformer xform;
        try {
            xform = tfactory.newTransformer();
            xform.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            Source src = new DOMSource(el);
            Result result = new StreamResult(writer);
            xform.transform(src, result);
        } catch (TransformerException e) {
            e.printStackTrace();
        }
        return writer.toString();
    }

    public static class Builder implements EventDeserializer.Builder {

        @Override
        public EventDeserializer build(Context context, ResettableInputStream resettableInputStream) {
            try {
                return new XmlXpathDeserializer(context, resettableInputStream);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

}
