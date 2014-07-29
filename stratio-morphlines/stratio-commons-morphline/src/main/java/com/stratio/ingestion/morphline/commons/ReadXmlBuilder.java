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
import java.io.StringReader;
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
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.stdio.AbstractParser;
import org.w3c.dom.Document;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSSerializer;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.google.common.collect.Maps;
import com.typesafe.config.Config;

//@formatter:off
/**
 * The readXml command parses an InputStream from field specified by field parameter (_attachment_body by default) 
 * and uses XPath expressions to extract fields and add them into headers.
 * Example:
 * {
 *     readXml {
 *        field : source
 *        paths : {
 *          book1 : "/catalog/book[@id='bk101']/author"
 *          book2 : "/catalog/book[@id='bk102']/genre"
 *        }
 *      }
 *    }
 * 
 * If paths field is empty (paths : { } ) whole xml will be parsed into a String with name _xml.
 */
//@formatter:on
public class ReadXmlBuilder implements CommandBuilder {

    private static final String CONF_PATHS = "paths";
    private static final String CONF_FIELD = "source";

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
        private boolean all = false;
        private String field;
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

            Config paths = getConfigs().getConfig(config, CONF_PATHS);
            for (Map.Entry<String, Object> entry : new Configs().getEntrySet(paths)) {
                String fieldName = entry.getKey();
                String path = entry.getValue().toString().trim();
                stepMap.put(fieldName, path);
            }

            field = getConfigs().getString(config, CONF_FIELD, null);

            if (stepMap.size() == 0) {
                all = true;
            }

            LOG.debug("stepMap: {}", stepMap);
        }

        @Override
        protected boolean doProcess(Record record, InputStream stream) throws IOException {
            Document doc = null;
            try {
                if (field == null) {
                    doc = docBuilder.parse(stream);
                } else if (record.get(field) != null) {
                    InputSource is = new InputSource(new StringReader(String.valueOf(record.get(
                            field).get(0))));
                    doc = docBuilder.parse(is);
                }
            } catch (SAXException e) {
                LOG.error("Cannot parse body");
                return false;
            }
            Record outputRecord = record.copy();
            if (all) {
                outputRecord.put("_xml", XMLtoString(doc));
                if (!getChild().process(outputRecord)) {
                    return false;
                }
            } else {
                for (Map.Entry<String, String> entry : stepMap.entrySet()) {
                    XPathExpression expr = null;
                    try {
                        expr = xpath.compile(entry.getValue());
                        String field = (String) expr.evaluate(doc, XPathConstants.STRING);
                        outputRecord.put(entry.getKey(), field);
                    } catch (XPathExpressionException e) {
                        LOG.error("Invalid XPATH expression -> " + expr);
                        return false;
                    }
                }
                if (!getChild().process(outputRecord)) {
                    return false;
                }
            }

            return true;
        }

        public String XMLtoString(Document doc) {
            DOMImplementationLS domImplementation = (DOMImplementationLS) doc.getImplementation();
            LSSerializer lsSerializer = domImplementation.createLSSerializer();
            return lsSerializer.writeToString(doc);
        }
    }
}
