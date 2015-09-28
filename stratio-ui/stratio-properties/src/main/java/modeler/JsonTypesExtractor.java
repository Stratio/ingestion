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
package modeler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.stratio.ingestion.model.Attribute;

/**
 * Created by miguelsegura on 24/09/15.
 */
public class JsonTypesExtractor {

//    private String path2 = "../../../../../stratio-jsons/java/org/apache/flume/";
//    private String path = "/home/miguelsegura/workspace/flume-ingestion/stratio-ui/stratio-jsons/java/org/apache"
//            + "/flume/";
    private JsonNode rootNode;
    private ObjectMapper mapper = new ObjectMapper();

    public JsonTypesExtractor(){}

    public Attribute typesExtractor(String component, String typeComponent, String atribComponent)
            throws IOException {

        Attribute type = new Attribute();

        try{
            if(component.equals("sources")){
                type = typeSources(typeComponent, atribComponent);

            }
            if(component.equals("sinks")){
                type = typeSinks(typeComponent, atribComponent);
            }
            if(component.equals("channels")){
                type = typeChannels(typeComponent, atribComponent);
            }
        } catch (Exception e) {
            throw new IOException("Cannot serialize JSON", e);

        }


        return type;

    }

    private Attribute getField(JsonNode rootNode, String atribComponent){
        Attribute typeField= new Attribute();

        JsonNode sourcesNode = rootNode.path("descriptors");

        JsonNode componentsNode = sourcesNode.path("components");
        Iterator<JsonNode> compIte = componentsNode.getElements();

        while (compIte.hasNext()) {
            JsonNode component = compIte.next();
            JsonNode settingsNode = component.path("settings");
            Iterator<JsonNode> settingsIte = settingsNode.getElements();
            JsonNode settingsList = settingsIte.next();
            JsonNode fields = settingsList.findPath(atribComponent);
            if(!fields.isNull()){
                Iterator<JsonNode> valuesField = fields.getElements();
                JsonNode fieldType = valuesField.next();
                JsonNode type = fieldType.findPath("type");
                JsonNode required = fieldType.findPath("required");
                typeField.setType(type.asText());
                typeField.setRequired(required.asBoolean());
            }else{
                typeField = null;
            }

        }

        return typeField;
    }

    private Attribute typeSources(String typeComponent, String atribComponent) throws IOException {

        try {
            Attribute type = new Attribute();

            if (typeComponent.equals("spoolSource")) {
                BufferedReader fileReader = new BufferedReader(
                        new FileReader("src/main/resources/source/spoolDirectory/spoolDirectorySource.json"));

                rootNode = mapper.readValue(fileReader, JsonNode.class);

                type = getField(rootNode, atribComponent);
            }
            if (typeComponent.equals("execSource")) {
                BufferedReader fileReader = new BufferedReader(
                        new FileReader("src/main/resources/source/exec/execSource.json"));

                rootNode = mapper.readValue(fileReader, JsonNode.class);

                type = getField(rootNode, atribComponent);
            }

            return type;
        }catch (Exception e) {
            throw new IOException("Cannot find type source", e);

        }
    }

    private Attribute typeSinks(String typeComponent, String atribComponent) throws IOException {

        try {
            Attribute type = new Attribute();

            if(typeComponent.equals("kafkaSink")){
                BufferedReader fileReader = new BufferedReader(
                        new FileReader("src/main/resources/sink/kafka/kafka.json"));

                rootNode = mapper.readValue(fileReader, JsonNode.class);

                type = getField(rootNode, atribComponent);

                if(type.equals(null)){
                    fileReader = new BufferedReader(
                            new FileReader("src/main/resources/sink/kafka/kafkaSink.json"));

                    rootNode = mapper.readValue(fileReader, JsonNode.class);

                    type = getField(rootNode, atribComponent);
                }
            }
            if(typeComponent.equals("cassandraSink")){
                BufferedReader fileReader = new BufferedReader(
                        new FileReader("src/main/resources/sink/cassandra/cassandra.json"));

                rootNode = mapper.readValue(fileReader, JsonNode.class);

                type = getField(rootNode, atribComponent);
            }

            return type;
        }catch (Exception e) {
            throw new IOException("Cannot find type source", e);

        }
    }

    private Attribute typeChannels(String typeComponent, String atribComponent) throws IOException {

        try {
            Attribute type = new Attribute();

            if(typeComponent.equals("memoryChannel")){
                BufferedReader fileReader = new BufferedReader(
                        new FileReader("src/main/resources/channel/memory/memoryChannel.json"));

                rootNode = mapper.readValue(fileReader, JsonNode.class);

                type = getField(rootNode, atribComponent);
            }
            if(typeComponent.equals("jdbcChannel")){
                BufferedReader fileReader = new BufferedReader(
                        new FileReader("src/main/resources/channel/jdbc/jdbcChannel.json"));

                rootNode = mapper.readValue(fileReader, JsonNode.class);

                type = getField(rootNode, atribComponent);
            }

            return type;
        }catch (Exception e) {
            throw new IOException("Cannot find type channel", e);

        }
    }

}
