package com.stratio.ingestion.source.generator;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.flume.ChannelException;

import java.util.List;

public class RandomFieldsGenerator {
   public static String generateRandomField(GeneratorField generatorField) throws ChannelException {
       StringBuilder randomString = new StringBuilder();
       String fieldType = generatorField.getType();
       switch (fieldType) {
           case "string" : randomString.append(generateRandomString(generatorField));
               break;
       }
      return randomString.toString();
   }

   private static String generateRandomString(GeneratorField generatorField) throws ChannelException {
       try {
        List<FieldProperty> properties = generatorField.getProperties();
        FieldProperty fieldProperty = properties.get(0);
        int length = Integer.parseInt(fieldProperty.getPropertyValue());
        return RandomStringUtils.randomAscii(length);
       } catch(NumberFormatException nFEx) {
           throw new ChannelException(nFEx.getMessage());
       }

   }
}
