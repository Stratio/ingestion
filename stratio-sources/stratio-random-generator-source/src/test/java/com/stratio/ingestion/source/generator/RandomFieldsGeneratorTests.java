package com.stratio.ingestion.source.generator;


import org.apache.commons.io.IOUtils;
import org.apache.flume.ChannelException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class RandomFieldsGeneratorTests {

    @Test
    public void testShouldGenerateRandomStringWithTheProperLength() throws Exception {
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
}
