/*
  * Copyright 2014 Stratio.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package com.stratio.ingestion.sink.stratiostreaming;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import com.stratio.ingestion.sink.stratiostreaming.StreamDefinition;
import com.stratio.ingestion.sink.stratiostreaming.StreamDefinitionParser;
import com.stratio.ingestion.sink.stratiostreaming.StreamField;

import java.util.List;

import static org.junit.Assert.*;

public class StreamDefinitionParserTest {
    
  @Test
  public void testParseDefinitionStreamFile() throws Exception {
     StreamDefinitionParser parser = new StreamDefinitionParser(IOUtils.toString(this.getClass()
                                .getResourceAsStream("/definition.json")));
     StreamDefinition streamDefinition = parser.parse();
     List<StreamField> listOfStreamFields = streamDefinition.getFields();
     assertEquals(streamDefinition.getStreamName(),"testStream");
     assertEquals(listOfStreamFields.size(), 2);
     assertEquals(listOfStreamFields.get(0).getName(), "field1");
     assertEquals(listOfStreamFields.get(0).getType(), "string");
     assertEquals(listOfStreamFields.get(1).getName(), "field2");
     assertEquals(listOfStreamFields.get(1).getType(), "int");
  }

    @Test
    public void testParseStreamType() throws Exception {
        StreamDefinitionParser parser = new StreamDefinitionParser(IOUtils.toString(this.getClass()
                .getResourceAsStream("/definition.json")));
        StreamDefinition streamDefinition = parser.parse();
        List<StreamField> listOfStreamFields = streamDefinition.getFields();
        assertEquals(streamDefinition.getStreamName(),"testStream");
        assertEquals(listOfStreamFields.size(), 2);
        assertEquals(listOfStreamFields.get(0).getName(), "field1");
        assertEquals(listOfStreamFields.get(0).getType(), "string");
        assertEquals(listOfStreamFields.get(1).getName(), "field2");
        assertEquals(listOfStreamFields.get(1).getType(), "int");
    }
}