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
package com.stratio.ingestion.source.generator;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.util.List;

import static org.apache.commons.io.IOUtils.*;
import static org.junit.Assert.assertEquals;

public class RandomGeneratorDefinitionParserTest {

    @Test
    public void testParseDefinitionStreamFile() throws Exception {
        RandomGeneratorDefinitionParser parser = new RandomGeneratorDefinitionParser(IOUtils.toString(this.getClass()
                .getResourceAsStream("/generator.json")));
        GeneratorDefinition generatorDefinition = parser.parse();
        List<GeneratorField> listOfStreamFields = generatorDefinition.getFields();
        assertEquals(listOfStreamFields.size(), 4);
        assertEquals(listOfStreamFields.get(0).getType(), "list");
        List<FieldProperty> listOfFieldPropertiesField1 = listOfStreamFields.get(0).getProperties();
        assertEquals(listOfFieldPropertiesField1.get(0).getPropertyName(), "values");
        assertEquals(listOfFieldPropertiesField1.get(0).getPropertyValue(), "testStream1, testStream2");
        assertEquals(listOfStreamFields.get(1).getType(), "integer");
        List<FieldProperty> listOfFieldPropertiesField2 = listOfStreamFields.get(1).getProperties();
        assertEquals(listOfFieldPropertiesField2.get(0).getPropertyName(), "length");
        assertEquals(listOfFieldPropertiesField2.get(0).getPropertyValue(), "4");
        assertEquals(listOfStreamFields.get(2).getType(), "string");
        List<FieldProperty> listOfFieldPropertiesField3 = listOfStreamFields.get(2).getProperties();
        assertEquals(listOfFieldPropertiesField3.get(0).getPropertyName(), "length");
        assertEquals(listOfFieldPropertiesField3.get(0).getPropertyValue(), "5");
    }

}
