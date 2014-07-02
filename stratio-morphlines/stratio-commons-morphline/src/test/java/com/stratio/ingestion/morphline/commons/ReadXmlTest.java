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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.Fields;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;

@RunWith(JUnit4.class)
public class ReadXmlTest {

    private static final String XML_FILE = "/xml/test.xml";
    private static final String MORPH_CONF_FILE = "/xml/readXml.conf";

    private Document doc;
    private Config config;

    @Before
    public void setUp() throws ParserConfigurationException, SAXException, IOException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder;
        docBuilder = factory.newDocumentBuilder();

        URL url = getClass().getResource(XML_FILE);
        File file = new File(url.getPath());

        doc = docBuilder.parse(file);
        doc.getDocumentElement().normalize();

        config = parse(MORPH_CONF_FILE).getConfigList("commands").get(0).getConfig("readXml");
    }

    @Test
    public void testConfiguration() throws URISyntaxException {
        ConfigObject object = config.getObject("paths");
        Set<String> keySet = object.keySet();
        List<String> values = new ArrayList<String>();

        for (String key : keySet) {
            ConfigValue configValue = object.get(key);
            values.add(configValue.render());
        }

        assertEquals(keySet.size(), 2);

        List<String> result = new ArrayList<String>();
        result.add(new String("\"/catalog/book[@id='bk101']/author\""));
        result.add(new String("\"/catalog/book[@id='bk102']/genre\""));
        assertTrue(values.equals(result));
    }

    @Test
    public void testRecordCreated() throws IOException {
        final MorphlineContext context = new MorphlineContext.Builder().build();
        Collector collectorParent = new Collector();
        Collector collectorChild = new Collector();
        final Command command = new ReadXmlBuilder().build(config, collectorParent, collectorChild,
                context);

        Record record = new Record();
        record.put(Fields.ATTACHMENT_BODY,
                new FileInputStream(new File(getClass().getResource(XML_FILE).getPath())));

        command.process(record);

        Record result = collectorChild.getRecords().get(0);
        assertEquals(result.get("book1").get(0),"Gambardella, Matthew");
    }

    protected Config parse(String file, Config... overrides) throws IOException {
        File tmpFile = File.createTempFile("morphlines_", ".conf");
        IOUtils.copy(getClass().getResourceAsStream(file), new FileOutputStream(tmpFile));
        Config config = new Compiler().parse(tmpFile, overrides);
        config = config.getConfigList("morphlines").get(0);
        Preconditions.checkNotNull(config);
        return config;
    }

}