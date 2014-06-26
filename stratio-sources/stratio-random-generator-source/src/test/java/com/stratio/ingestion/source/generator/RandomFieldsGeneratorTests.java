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


import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.flume.ChannelException;
import org.junit.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

public class RandomFieldsGeneratorTests {

    @Test
    public void testShouldGenerateRandomStringsWithTheProperLength() throws Exception {
        GeneratorField stringField = new GeneratorField();
        stringField.setType("string");
        List<FieldProperty> properties = new ArrayList<>();
        FieldProperty lengthProperty = new FieldProperty();
        lengthProperty.setPropertyName("length");
        lengthProperty.setPropertyValue("4");
        properties.add(lengthProperty);
        stringField.setProperties(properties);
        String randomStringGenerated = RandomFieldsGenerator.generateRandomField(stringField);
        assertEquals(randomStringGenerated.length(), 4);
    }

    @Test(expected=ChannelException.class)
    public void testShouldThrowAChannelExceptionWhenTheValueOfLenghtIsNotValid() throws Exception {
        GeneratorField stringField = new GeneratorField();
        stringField.setType("string");
        List<FieldProperty> properties = new ArrayList<>();
        FieldProperty lengthProperty = new FieldProperty();
        lengthProperty.setPropertyName("length");
        lengthProperty.setPropertyValue("wrongNumericProperty");
        properties.add(lengthProperty);
        stringField.setProperties(properties);
        RandomFieldsGenerator.generateRandomField(stringField);
    }

    @Test
    public void testShouldGenerateRandomIntegersWithTheProperLength() throws Exception {
        GeneratorField integerField = new GeneratorField();
        integerField.setType("integer");
        List<FieldProperty> properties = new ArrayList<>();
        FieldProperty lengthProperty = new FieldProperty();
        lengthProperty.setPropertyName("length");
        lengthProperty.setPropertyValue("4");
        properties.add(lengthProperty);
        integerField.setProperties(properties);
        String randomIntegerGenerated = RandomFieldsGenerator.generateRandomField(integerField);
        assertEquals(randomIntegerGenerated.length(), 4);
        boolean thrown = false;
        try {
            Integer.parseInt(randomIntegerGenerated);
        } catch (NumberFormatException e) {
            thrown = true;
        }
        assertFalse(thrown);
    }

    @Test(expected=ChannelException.class)
    public void testShouldThrowAChannelExceptionWhenTheValueOfIntegerLenghtIsNotValid() throws Exception {
        GeneratorField integerField = new GeneratorField();
        integerField.setType("integer");
        List<FieldProperty> properties = new ArrayList<>();
        FieldProperty lengthProperty = new FieldProperty();
        lengthProperty.setPropertyName("length");
        lengthProperty.setPropertyValue("wrongNumericProperty");
        properties.add(lengthProperty);
        integerField.setProperties(properties);
        RandomFieldsGenerator.generateRandomField(integerField);
    }

    @Test
    public void testShouldReturnAListOfStringsFromTheStringList() throws Exception {
        GeneratorField listField = new GeneratorField();
        listField.setType("list");
        List<FieldProperty> properties = new ArrayList<>();
        FieldProperty lengthProperty = new FieldProperty();
        lengthProperty.setPropertyName("values");
        lengthProperty.setPropertyValue("testStream1, testStream2, testStream3");
        properties.add(lengthProperty);
        listField.setProperties(properties);
        String randomListValue = RandomFieldsGenerator.generateRandomField(listField);
        boolean streamGeneratedFromList = (randomListValue.equals("testStream1") || randomListValue.equals("testStream2") || randomListValue.equals("testStream3"));
        assertTrue(streamGeneratedFromList);
    }

    @Test
    public void testShouldReturnAValidIP() throws Exception {
        GeneratorField ipField = new GeneratorField();
        ipField.setType("ip");
        String randomIpValue = RandomFieldsGenerator.generateRandomField(ipField);
        assertTrue(InetAddressValidator.getInstance().isValid(randomIpValue));
    }

    @Test
    public void testShouldGenerateProperDates() throws Exception {
        GeneratorField stringField = new GeneratorField();
        String dateFormat = "dd/MMM/yyyy:hh:mm:ss Z";
        stringField.setType("date");
        List<FieldProperty> properties = new ArrayList<>();
        FieldProperty lengthProperty = new FieldProperty();
        lengthProperty.setPropertyName("dateFormat");
        lengthProperty.setPropertyValue(dateFormat);
        properties.add(lengthProperty);
        stringField.setProperties(properties);
        String randomDateGenerated = RandomFieldsGenerator.generateRandomField(stringField);
        DateFormat df = new SimpleDateFormat(dateFormat);
        boolean thrown = false;
        try {
            df.parse(randomDateGenerated);
        } catch (ParseException e) {
            thrown = true;
        }
        assertFalse(thrown);
    }
}
