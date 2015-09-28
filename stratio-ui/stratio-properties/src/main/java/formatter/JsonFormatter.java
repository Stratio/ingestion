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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;

import com.google.gson.JsonObject;
import com.stratio.ingestion.model.AgentComponent;
import com.stratio.ingestion.model.Attribute;
import com.stratio.ingestion.model.channel.Channel;
import com.stratio.ingestion.model.sink.Sink;
import com.stratio.ingestion.model.source.Source;

import modeler.JsonTypesExtractor;

/**
 * Created by miguelsegura on 24/09/15.
 */
public class JsonFormatter {

//    private AgentComponent agentComponent = new AgentComponent();
    private JsonTypesExtractor jsonType = new JsonTypesExtractor();

    public JsonFormatter(){}

    public JsonFormatter(AgentComponent agent) throws IOException {
        try {



            String rutaArchivo = "src/test"
                    + "/resources/js.json";
            File archivo = new File(rutaArchivo);
            BufferedWriter bw;

            bw = new BufferedWriter(new FileWriter(archivo));


            writeJson(bw, agent);


            bw.close();


        }catch (Exception e) {
            throw new IOException("Cannot serialize JSON", e);
        }
    }


    public void writeJson(BufferedWriter bw, AgentComponent agentComponent) throws IOException {

        List<Source> sourcesList = new ArrayList<>();
        List<Sink> sinksList = new ArrayList<>();
        List<Channel> channelsList = new ArrayList<>();
        List<Attribute> attributesList = new ArrayList<>();

        JSONObject obj = new JSONObject();
        JsonObject objectSource = new JsonObject();

        bw.write("{");
        bw.newLine();
        bw.write("\"sources\": {");
        bw.newLine();
        bw.write("\"components\": [{");
        bw.newLine();

        int countSources = 0;
        sourcesList = agentComponent.getSources();

        for(Source source : sourcesList){
            String idSource = source.getId();
            String typeSource = source.getType();
            String nameSource = source.getName();
            String descripSource = source.getDescription();
            attributesList = source.getSettings();
            String channelsSource = source.getChannels();

            bw.write("\"id\": \"" + idSource + "\",");
            bw.newLine();
            bw.write("\"type\": \"" + typeSource + "\",");
            bw.newLine();
            bw.write("\"name\": \"" + nameSource + "\",");
            bw.newLine();
            bw.write("\"description\": \"" + descripSource + "\",");
            bw.newLine();
            bw.write("\"channels\": \"" + channelsSource + "\",");
            bw.newLine();
            bw.write("\"settings\": [{");
            bw.newLine();

            int countSources2 = 0;

            for(Attribute atrib : attributesList){
                String idAtrib = atrib.getId();
                String typeAtrib = atrib.getType();
                String nameAtrib = atrib.getName();
                String descripAtrib = atrib.getDescription();
                Boolean requiredAtrib = atrib.getRequired();
                String stringValue = atrib.getValueString();
                Integer intValue = atrib.getValueInteger();
                Boolean booleanValue = atrib.getValueBoolean();

                bw.write("\"" + idAtrib + "\": [{");
                bw.newLine();
                bw.write("\"type\": \"" + typeAtrib + "\",");
                bw.newLine();
                bw.write("\"name\": \"" + nameAtrib + "\",");
                bw.newLine();
                bw.write("\"description\": \"" + descripAtrib + "\",");
                bw.newLine();
                bw.write("\"required\": " + requiredAtrib + ",");
                bw.newLine();
                if(typeAtrib.equals("string")){
                    bw.write("\"value\": \""+ stringValue+"\"");
                }
                if(typeAtrib.equals("integer")){
                    bw.write("\"value\": "+ intValue);
                }
                if(typeAtrib.equals("boolean")){
                    bw.write("\"value\": " + booleanValue);
                }
                bw.newLine();
                countSources2++;
                if(countSources2 == attributesList.size()){
                    bw.write("}]");
                }else{
                    bw.write("}],");
                }

                bw.newLine();

            }

            bw.write("}]");
            bw.newLine();
            countSources++;


            if(countSources == sourcesList.size()){
                bw.write("}]");
            }else{
                bw.write("},");
                bw.newLine();
                bw.write("{");
            }

            bw.newLine();
        }


        bw.write("},");
        bw.newLine();


        bw.write("\"sinks\": {");
        bw.newLine();
        bw.write("\"components\": [{");
        bw.newLine();

        int countSinks = 0;
        sinksList = agentComponent.getSinks();

        for(Sink sink : sinksList) {
            String idSink = sink.getId();
            String typeSink = sink.getType();
            String nameSink = sink.getName();
            String descripSink = sink.getDescription();
            attributesList = sink.getSettings();
            String channelsSink = sink.getChannels();

            bw.write("\"id\": \"" + idSink + "\",");
            bw.newLine();
            bw.write("\"type\": \"" + typeSink + "\",");
            bw.newLine();
            bw.write("\"name\": \"" + nameSink + "\",");
            bw.newLine();
            bw.write("\"description\": \"" + descripSink + "\",");
            bw.newLine();
            bw.write("\"channels\": \"" + channelsSink + "\",");
            bw.newLine();
            bw.write("\"settings\": [{");
            bw.newLine();

            int countSinks2 = 0;

            for(Attribute atrib : attributesList){
                String idAtrib = atrib.getId();
                String typeAtrib = atrib.getType();
                String nameAtrib = atrib.getName();
                String descripAtrib = atrib.getDescription();
                Boolean requiredAtrib = atrib.getRequired();
                String stringValue = atrib.getValueString();
                Integer intValue = atrib.getValueInteger();
                Boolean booleanValue = atrib.getValueBoolean();

                bw.write("\"" + idAtrib + "\": [{");
                bw.newLine();
                bw.write("\"type\": \"" + typeAtrib + "\",");
                bw.newLine();
                bw.write("\"name\": \"" + nameAtrib + "\",");
                bw.newLine();
                bw.write("\"description\": \"" + descripAtrib + "\",");
                bw.newLine();
                bw.write("\"required\": " + requiredAtrib + ",");
                bw.newLine();
                if(typeAtrib.equals("string")){
                    bw.write("\"value\": \""+ stringValue+"\"");
                }
                if(typeAtrib.equals("integer")){
                    bw.write("\"value\": "+ intValue);
                }
                if(typeAtrib.equals("boolean")){
                    bw.write("\"value\": " + booleanValue);
                }
                bw.newLine();
                countSinks2++;
                if(countSinks2 == attributesList.size()){
                    bw.write("}]");
                }else{
                    bw.write("}],");
                }

                bw.newLine();

            }

            bw.write("}]");
            bw.newLine();
            countSinks++;


            if(countSinks == sinksList.size()){
                bw.write("}]");
            }else{
                bw.write("},");
                bw.newLine();
                bw.write("{");
            }

            bw.newLine();
        }

        bw.write("},");
        bw.newLine();


        bw.write("\"channels\": {");
        bw.newLine();
        bw.write("\"components\": [{");
        bw.newLine();

        int countChannels = 0;
        channelsList = agentComponent.getChannels();

        for(Channel channel : channelsList) {
            String idChannel = channel.getId();
            String typeChannel = channel.getType();
            String nameChannel = channel.getName();
            String descripChannel = channel.getDescription();
            attributesList = channel.getSettings();
            String sourcesChannel = channel.getSources();

            bw.write("\"id\": \"" + idChannel + "\",");
            bw.newLine();
            bw.write("\"type\": \"" + typeChannel + "\",");
            bw.newLine();
            bw.write("\"name\": \"" + nameChannel + "\",");
            bw.newLine();
            bw.write("\"description\": \"" + descripChannel + "\",");
            bw.newLine();
            bw.write("\"sources\": \"" + sourcesChannel + "\",");
            bw.newLine();
            bw.write("\"settings\": [{");
            bw.newLine();

            int countChannel2 = 0;

            for(Attribute atrib : attributesList){
                String idAtrib = atrib.getId();
                String typeAtrib = atrib.getType();
                String nameAtrib = atrib.getName();
                String descripAtrib = atrib.getDescription();
                Boolean requiredAtrib = atrib.getRequired();
                String stringValue = atrib.getValueString();
                Integer intValue = atrib.getValueInteger();
                Boolean booleanValue = atrib.getValueBoolean();

                bw.write("\"" + idAtrib + "\": [{");
                bw.newLine();
                bw.write("\"type\": \"" + typeAtrib + "\",");
                bw.newLine();
                bw.write("\"name\": \"" + nameAtrib + "\",");
                bw.newLine();
                bw.write("\"description\": \"" + descripAtrib + "\",");
                bw.newLine();
                bw.write("\"required\": " + requiredAtrib + ",");
                bw.newLine();
                if(typeAtrib.equals("string")){
                    bw.write("\"value\": \""+ stringValue+"\"");
                }
                if(typeAtrib.equals("integer")){
                    bw.write("\"value\": "+ intValue);
                }
                if(typeAtrib.equals("boolean")){
                    bw.write("\"value\": " + booleanValue);
                }
                bw.newLine();
                countChannel2++;
                if(countChannel2 == attributesList.size()){
                    bw.write("}]");
                }else{
                    bw.write("}],");
                }

                bw.newLine();

            }

            bw.write("}]");
            bw.newLine();
            countChannels++;


            if(countChannels == channelsList.size()){
                bw.write("}]");
            }else{
                bw.write("},");
                bw.newLine();
                bw.write("{");
            }

            bw.newLine();

        }


        bw.write("}");
        bw.newLine();
        bw.write("}");
        bw.newLine();
    }

}

















