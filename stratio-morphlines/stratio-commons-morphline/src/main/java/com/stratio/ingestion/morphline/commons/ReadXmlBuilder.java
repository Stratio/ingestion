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
package com.stratio.ingestion.morphline.commons;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.stdio.AbstractParser;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import com.google.common.collect.Maps;
import com.typesafe.config.Config;

public class ReadXmlBuilder implements CommandBuilder {

    private static final String PATHS_CONF = "paths";

    @Override
    public Collection<String> getNames() {
        return Collections.singletonList("readXml");
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new ReadXml(this, config, parent, child, context);
    }

    private static final class ReadXml extends AbstractParser {

        private final Map<String, String> stepMap;
        private final XPath xpath;

        private DocumentBuilder docBuilder;

        protected ReadXml(CommandBuilder builder, Config config, Command parent, Command child,
                MorphlineContext context) {
            super(builder, config, parent, child, context);

            xpath = XPathFactory.newInstance().newXPath();
            stepMap = Maps.newHashMap();

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            try {
                docBuilder = factory.newDocumentBuilder();
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            }

            Config paths = getConfigs().getConfig(config, PATHS_CONF);
            for (Map.Entry<String, Object> entry : new Configs().getEntrySet(paths)) {
                String fieldName = entry.getKey();
                String path = entry.getValue().toString().trim();
                stepMap.put(fieldName, path);
            }

            LOG.debug("stepMap: {}", stepMap);
        }

        @Override
        protected boolean doProcess(Record record, InputStream stream) throws IOException {
            Record template = record.copy();
            removeAttachments(template);
            Document doc = null;
            try {
                doc = docBuilder.parse(stream);
            } catch (SAXException e) {
                LOG.error("Cannot parse body");
                return false;
            }

            Record outputRecord = template.copy();
            for (Map.Entry<String, String> entry : stepMap.entrySet()) {
                XPathExpression expr = null;
                try {
                    expr = xpath.compile(entry.getValue());
                    String field = (String)expr.evaluate(doc, XPathConstants.STRING);
                    outputRecord.put(entry.getKey(), field);
                    if (!getChild().process(outputRecord)) {
                      return false;
                    }
                } catch (XPathExpressionException e) {
                    LOG.error("Invalid XPATH expression -> " + expr);
                    return false;
                }
            }
            return true;
        }

        public static void print(Node node, OutputStream os) {
            PrintStream ps = new PrintStream(os);
            switch (node.getNodeType()) {
                case Node.ELEMENT_NODE:
                    ps.print("<" + node.getNodeName());

                    NamedNodeMap map = node.getAttributes();
                    for (int i = 0; i < map.getLength(); i++) {
                        ps.print(" " + map.item(i).getNodeName() + "=\""
                                + map.item(i).getNodeValue() + "\"");
                    }
                    ps.println(">");
                    return;
                case Node.ATTRIBUTE_NODE:
                    ps.println(node.getNodeName() + "=\"" + node.getNodeValue() + "\"");
                    return;
                case Node.TEXT_NODE:
                    ps.println(node.getNodeValue());
                    return;
                case Node.CDATA_SECTION_NODE:
                    ps.println(node.getNodeValue());
                    return;
                case Node.PROCESSING_INSTRUCTION_NODE:
                    ps.println(node.getNodeValue());
                    return;
                case Node.DOCUMENT_NODE:
                case Node.DOCUMENT_FRAGMENT_NODE:
                    ps.println(node.getNodeName() + "=" + node.getNodeValue());
                    return;
            }
        }
    }
}
