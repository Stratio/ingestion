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
/**
 * Created by miguelsegura on 15/09/15.
 */

package jsonFormatter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class FieldsValidator {
    private static final Logger log = LoggerFactory.getLogger(FieldsValidator.class);

    private List list = null;
    private String element;

    public FieldsValidator(String jsonFile) throws IOException {

        try {

            ObjectMapper mapper = new ObjectMapper();

            BufferedReader fileReader = new BufferedReader(
                    new FileReader(jsonFile));

            JsonNode rootNode = mapper.readValue(fileReader, JsonNode.class);

            element = "sources";
            checkProperties(rootNode, element);

            element = "sinks";
            checkProperties(rootNode, element);

            element = "channels";
            checkProperties(rootNode, element);



            //MIERCOLES
            JsonParser parser = new JsonParser();
            FileReader file = new FileReader(jsonFile);
            JsonElement datos = parser.parse(file);
            JsonObject jobject = datos.getAsJsonObject();

            dumpJSONElement(datos);

            //COMPROBACION DE SOURCES
            jobject = jobject.getAsJsonObject("sources");
            JsonArray arrayComponents = jobject.getAsJsonArray("components");


            for (int i = 0; i < arrayComponents.size(); i++) {
                jobject = arrayComponents.get(i).getAsJsonObject();

                JsonArray listaSettings = jobject.get("settings").getAsJsonArray();
                JsonObject jobject2 = listaSettings.get(0).getAsJsonObject();


                JsonArray host = jobject2.get("hostname").getAsJsonArray();
                JsonObject jsonObject3 = host.get(0).getAsJsonObject();
                String descrip = jsonObject3.get("description").getAsString();

            }

//            String listaSettings = jobject.get("settings").toString();
//            System.out.println(listaSettings);
//            System.out.println("************************************************************************");
//            JsonArray listaSettings2 = jobject.get("settings").getAsJsonArray();
//            JsonObject jobject2 = listaSettings2.get(0).getAsJsonObject();
//            JsonArray host = jobject2.get("hostname").getAsJsonArray();
//            JsonObject jsonObject3 = host.get(0).getAsJsonObject();
//            String descrip = jsonObject3.get("description").getAsString();
////            String host = jobject2.get("hostname").toString();
//            System.out.println(descrip);


            System.out.println("************************************************************************");


            return;
        } catch (Exception e) {
            throw new IOException("Cannot serialize JSON", e);
        } finally {

            //            try {
            //                in.close();
            //            } catch (IOException ex) {
            //                log.warn("Error while closing input stream");
            //            }
        }
    }

    private String formatString(String inputString){

        inputString = inputString.replace("\"", "");
        inputString = inputString.replace("[", "");
        inputString = inputString.replace("]", "");
        inputString = inputString.replace(",", " ");

        return inputString;
    }

    private boolean checkTypes(String type, String value){

        boolean isCorrectType = false;

//        char first = Character.toUpperCase(type.charAt(0));
//        type = first + type.substring(1);
//        type = formatString(type);
//        System.out.println(type);
//        System.out.println();


        if (type.equals("string")){
            isCorrectType = value.getClass().equals(String.class);
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


        System.out.println(type);
        System.out.println();
        System.out.println(value);
        System.out.println();
        System.out.println();

        return isCorrectType;
    }

    private void checkProperties(JsonNode rootNode, String elements) throws IOException {
        JsonNode sourcesNode = rootNode.path(elements);

        JsonNode componentsNode = sourcesNode.path("components");

        Iterator<JsonNode> compIte = componentsNode.getElements();
        while (compIte.hasNext()) {
            JsonNode component = compIte.next();

            JsonNode settingsNode = component.path("settings");
            Iterator<JsonNode> settingsIte = settingsNode.getElements();
            JsonNode settingsList = settingsIte.next();

//            List<String> fieldList = new ArrayList<>();
//            Iterator<String> fieldNames = settingsList.getFieldNames();
//            while (fieldNames.hasNext()) {
//                String field = fieldNames.next();
//                String fieldName = field.toString();
//                fieldList.add(fieldName);
//            }

            List<String> valueList = new ArrayList<>();
            Iterator<JsonNode> settings = settingsList.getElements();
            while (settings.hasNext()) {
                JsonNode set = settings.next();
                Iterator<JsonNode> valuesIte = set.getElements();
                JsonNode values = valuesIte.next();

                JsonNode typeField = values.path("type");
                JsonNode valueField = values.path("value");

                String type = typeField.toString();
                type = formatString(type);
                String value = valueField.toString();
                value = formatString(value);

                checkTypes(type, value);
            }

//            HashMap<String, String> mappedFiles = new HashMap<>();
//
//            for (int i = 0; i < valueList.size(); i++) {
//                if (!(valueList.get(i).equals("null")) && !(valueList.get(i).equals(""))) {
//                    mappedFiles.put(fieldList.get(i), valueList.get(i));
//                }
//            }
//
//            Iterator properties = mappedFiles.entrySet().iterator();
//            while (properties.hasNext()) {
//                Map.Entry line = (Map.Entry) properties.next();
//
//            }
        }
    }
    public static void dumpJSONElement(JsonElement elemento) {
        if (elemento.isJsonObject()) {
            System.out.println("Es objeto");
            JsonObject obj = elemento.getAsJsonObject();
//            String req = obj.get("settings").getAsString();
//            System.out.println("AQUI!!!!!!");
            java.util.Set<java.util.Map.Entry<String,JsonElement>> entradas = obj.entrySet();
            java.util.Iterator<java.util.Map.Entry<String,JsonElement>> iter = entradas.iterator();
            while (iter.hasNext()) {
                java.util.Map.Entry<String,JsonElement> entrada = iter.next();
                System.out.println("Clave: " + entrada.getKey());
                System.out.println("Valor:" + entrada.getValue());
                String clave = entrada.getKey();
                String valor = entrada.getValue().toString();

                if(clave.equals("required") && valor.equals("true")){
//                    if(!entrada.getValue().equals(null)){
                        System.out.println("Esta lleno:" + entrada.getValue());
//                    }
                }
//                if(!jelem.isJsonArray()) {
//                    JsonObject jobject = jelem.getAsJsonObject();
//                }
//                String req = jobject.get("required").getAsString();
//                if(req.equals("true")){
//                    String value = jobject.get("value").getAsString();
//                    if(value.equals(null)){
//                        System.out.println("No hay valor");
//                    }
//                }
                dumpJSONElement(entrada.getValue());
            }

        } else if (elemento.isJsonArray()) {
            JsonArray array = elemento.getAsJsonArray();
            System.out.println("Es array. Numero de elementos: " + array.size());
            if(array.size() == 6){

            }
            java.util.Iterator<JsonElement> iter = array.iterator();
            while (iter.hasNext()) {
                JsonElement entrada = iter.next();
                System.out.println(entrada);
                dumpJSONElement(entrada);
            }
//        } else if (elemento.isJsonPrimitive()) {
//            System.out.println("Es primitiva");
//            JsonPrimitive valor = elemento.getAsJsonPrimitive();
//            if (valor.isBoolean()) {
//                System.out.println("Es booleano: " + valor.getAsBoolean());
//            } else if (valor.isNumber()) {
//                System.out.println("Es numero: " + valor.getAsNumber());
//            } else if (valor.isString()) {
//                System.out.println("Es texto: " + valor.getAsString());
//            }
        } else if (elemento.isJsonNull()) {
            System.out.println("Es NULL");
        } else {
            System.out.println("Es otra cosa");
        }
    }



}