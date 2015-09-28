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
package formatter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class PropertiesFormatter
         {
    private static final Logger log = LoggerFactory.getLogger(PropertiesFormatter.class);

    private String element;

    public PropertiesFormatter(String jsonFile)
            throws IOException {

        try {

            String ruta = "src/test"
                    + "/resources/prop.properties";
            File archivo = new File(ruta);
            BufferedWriter bw;

            bw = new BufferedWriter(new FileWriter(archivo));


            ObjectMapper mapper = new ObjectMapper();

            BufferedReader fileReader = new BufferedReader(
                    new FileReader(jsonFile));

            JsonNode rootNode = mapper.readValue(fileReader, JsonNode.class);


            bw.write("#Name the components on this agent");
            bw.newLine();
            bw.newLine();

            /*** Create Sources ***/
            element = "sources";
            bw.write("##### " + element.toUpperCase() + " #####");
            bw.newLine();
            bw.newLine();
            createPropierties(rootNode, bw, element);

            /*** Create Sinks ***/
            element = "sinks";
            bw.write("##### " + element.toUpperCase() + " #####");
            bw.newLine();
            bw.newLine();
            createPropierties(rootNode, bw, element);

            /*** Create Channels ***/
            element = "channels";
            bw.write("##### " + element.toUpperCase() + " #####");
            bw.newLine();
            bw.newLine();
            createPropierties(rootNode, bw, element);

            /*** Join Elements ***/
            bw.write("##### UNION #####");
            bw.newLine();
            bw.newLine();
            element = "sources";
            joinElements(rootNode, bw, element);

            element = "sinks";
            joinElements(rootNode, bw, element);


            bw.close();

        } catch (Exception e) {
            throw new IOException("Cannot serialize JSON", e);
        }
    }


    private String formatString(String inputString){

        inputString = inputString.replace("\"", "");
        inputString = inputString.replace("[", "");
        inputString = inputString.replace("]", "");

        return inputString;
    }

    private boolean checkTypes(String type, JsonNode value){

         boolean isCorrectType = false;



         if (type.equals("string") && !value.equals("null")){
             isCorrectType = value.isTextual();//getClass().equals(String.class);
         }
         if (type.equals("boolean")){
             isCorrectType = value.getClass().equals(Boolean.class);
         }
         if (type.equals("integer")){
             isCorrectType = value.getClass().equals(Integer.class);
         }
         if (type.equals("char")){
             isCorrectType = value.getClass().equals(Character.class);
         }
         if (type.equals("byte")){
             isCorrectType = value.getClass().equals(Byte.class);
         }

         return isCorrectType;
    }

    private void createPropierties(JsonNode rootNode, BufferedWriter bw, String elements) throws IOException {
        JsonNode sourcesNode = rootNode.path(elements);

        JsonNode componentsNode = sourcesNode.path("components");
        Iterator<JsonNode> compIte = componentsNode.getElements();

        String idSources = "";
        while (compIte.hasNext()) {
            JsonNode component = compIte.next();
            JsonNode id = component.path("id");
            idSources = idSources +" "+ id.getTextValue();
        }


        bw.write("a1." + elements + "=" + idSources);
        bw.newLine();
        bw.newLine();

        compIte = componentsNode.getElements();
        while (compIte.hasNext()) {
            JsonNode component = compIte.next();
            JsonNode id = component.path("id");
            JsonNode type = component.path("type");
            bw.write("a1."+elements+"." + id.getTextValue() + ".type=" + type.getTextValue());
            bw.newLine();

            JsonNode settingsNode = component.path("settings");
            Iterator<JsonNode> settingsIte = settingsNode.getElements();
            JsonNode settingsList = settingsIte.next();

            List<String> fieldList = new ArrayList<>();
            Iterator<String> fieldNames = settingsList.getFieldNames();
            while (fieldNames.hasNext()) {
                String field = fieldNames.next();
                String fieldName = field.toString();
                fieldList.add(fieldName);
            }

            List<String> valueList = new ArrayList<>();
            Iterator<JsonNode> settings = settingsList.getElements();
            while (settings.hasNext()) {
                JsonNode set = settings.next();
                Iterator<JsonNode> valuesIte = set.getElements();
                JsonNode values = valuesIte.next();
                JsonNode typeField = values.path("type");
                JsonNode valueField = values.path("value");
                String typeString = typeField.toString();
                typeString = formatString(typeString);
                checkTypes(typeString, valueField);
                String value = valueField.toString();
                value = formatString(value);
                valueList.add(value);
            }

            HashMap<String, String> mappedFiles = new HashMap<>();

            for (int i = 0; i < valueList.size(); i++) {
                if (!(valueList.get(i).equals("null")) && !(valueList.get(i).equals(""))) {
                    mappedFiles.put(fieldList.get(i), valueList.get(i));
                }
            }

            Iterator properties = mappedFiles.entrySet().iterator();
            while (properties.hasNext()) {
                Map.Entry line = (Map.Entry) properties.next();

                bw.write("a1."+elements+"." + id.getTextValue() + "." + line.getKey() + "=" + line.getValue());
                bw.newLine();
            }
            bw.newLine();
        }


        bw.newLine();
    }

    private void joinElements(JsonNode rootNode, BufferedWriter bw, String elements) throws IOException{
        JsonNode sourcesNode = rootNode.path(elements);

        JsonNode componentsNode = sourcesNode.path("components");
        Iterator<JsonNode> compIte = componentsNode.getElements();

        while (compIte.hasNext()) {
            JsonNode component = compIte.next();
            JsonNode id = component.path("id");
            JsonNode channels = component.path("channels");
            bw.write("a1." + elements + "." + id.getTextValue() + ".channels=" + channels.getTextValue());
            bw.newLine();
        }
    }
}
