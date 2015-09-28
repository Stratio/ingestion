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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.ContentHandler;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.stratio.ingestion.model.AgentComponent;
import com.stratio.ingestion.model.Attribute;
import com.stratio.ingestion.model.channel.Channel;
import com.stratio.ingestion.model.sink.Sink;
import com.stratio.ingestion.model.source.Source;

import nu.studer.java.util.OrderedProperties;


/**
 * Created by miguelsegura on 23/09/15.
 */
public class PropertiesModeler {

    private AgentComponent agentComponent = new AgentComponent();
    private JsonTypesExtractor jsonType = new JsonTypesExtractor();

    public PropertiesModeler(){}

    public AgentComponent propertiesAgentModeler(String ruta) throws IOException, NumberFormatException {
        try {

            ContentHandler handler = null;
            Properties prop = new Properties();
            String propFileName = ruta;


            fillAttributes(ruta);


            return agentComponent;
        }catch (IOException e) {
            throw new IOException("Cannot serialize JSON", e);
        }catch (NumberFormatException e) {
            throw new NumberFormatException("Null Value JSON");
        }
    }

    public void fillAttributes(String rutaArchivo) throws IOException, NumberFormatException {
        try {
            OrderedProperties properties = new OrderedProperties();
            properties.load(new FileInputStream(new File(rutaArchivo)));
            Set<Map.Entry<String, String>> value = properties.entrySet();

//            Iterator<Map.Entry<String, String>> components = value.iterator();
            Iterator<Map.Entry<String, String>> attribs = value.iterator();

            String componentOld = "";
            String atribComponent = "";
            String typeComponentOld = "";
//            boolean isConnected = false;

            Source source;
            Sink sink;
            Channel channel;
            List<Attribute> attribList = new ArrayList<>();
            List<Source> sourcesList = new ArrayList<>();
            List<Sink> sinksList = new ArrayList<>();
            List<Channel> channelsList = new ArrayList<>();

            while (attribs.hasNext()) {
                Map.Entry<String, String> next = attribs.next();

                String key = next.getKey();
                String val = next.getValue();
                String[] keys = key.split("\\.");

                String typeComponent = keys[1].toString();

                if(!typeComponent.equals("sources") && !typeComponent.equals("sinks") && !typeComponent.equals
                        ("channels")){
                    throw new IOException("Cannot serialize JSON, incorrect component");
                }

                if (keys.length >= 3) {

                    String componentNew = keys[2].toString();
                    atribComponent = keys[3].toString();

                    for(int i=4; i<keys.length; i++){
                        atribComponent = atribComponent.concat("."+keys[i]);
                    }


                    if(!componentOld.equals(componentNew) && !atribComponent.equals("channels")){
                        if(componentOld.contains("Source")){
                            source = new Source();
                            source.setId(componentOld);
                            source.setType(typeComponentOld);
                            source.setSettings(attribList);
                            attribList = new ArrayList<>();
                            sourcesList.add(source);
                        }
                        if (componentOld.contains("Sink")) {
                            agentComponent.setSources(sourcesList);
                            sink = new Sink();
                            sink.setId(componentOld);
                            sink.setType(typeComponentOld);
                            sink.setSettings(attribList);
                            attribList = new ArrayList<>();
                            sinksList.add(sink);
                        }
                        if (componentOld.contains("Channel")) {
                            agentComponent.setSinks(sinksList);
                            channel = new Channel();
                            channel.setId(componentOld);
                            channel.setType(typeComponentOld);
                            channel.setSettings(attribList);
                            attribList = new ArrayList<>();
                            channelsList.add(channel);
                        }
                        typeComponentOld = typeComponent;
                        componentOld = componentNew;
                    }
                    if(!componentOld.equals(componentNew) && componentNew.equals("spoolSource")){
                        channel = new Channel();
                        channel.setId(componentOld);
                        channel.setType(typeComponent);
                        channel.setSettings(attribList);
                        attribList = new ArrayList<>();
                        channelsList.add(channel);
                    }

                    if (!atribComponent.equals("type") && !atribComponent.equals("channels")) {
                        Attribute atrib = new Attribute();
                        atrib.setId(atribComponent);
                        Attribute types = jsonType.typesExtractor(typeComponent, componentNew, atribComponent);
                        String typeAtrib = types.getType();
                        Boolean requiredAtrib = types.getRequired();

                        if (typeAtrib.equals("string")) {
                            atrib.setType("string");
                            atrib.setRequired(requiredAtrib);
                            atrib.setValueString(val);
                            attribList.add(atrib);
//                            System.out.println(atrib.getValueString().toString());
                        }
                        if (typeAtrib.equals("integer")) {
                            atrib.setType("integer");
                            atrib.setRequired(requiredAtrib);
                            atrib.setValueInteger(Integer.parseInt(val));
                            attribList.add(atrib);
//                            System.out.println(atrib.getValueInteger().toString());
                        }
                        if (typeAtrib.equals("boolean")) {
                            atrib.setType("boolean");
                            atrib.setRequired(requiredAtrib);
                            atrib.setValueBoolean(Boolean.parseBoolean(val));
                            attribList.add(atrib);
//                            System.out.println(atrib.getValueBoolean().toString());
                        }
                    }

                    if(atribComponent.equals("channels")){
                        if(componentNew.contains("Source")){
                            List<Source> sourcesChannel = agentComponent.getSources();

                            connectChannelSource(sourcesChannel, val, componentNew, channelsList);

                        }
                        if(componentNew.contains("Sink")){
                            List<Sink> sinksChannel = agentComponent.getSinks();

                            connectChannelSink(sinksChannel, val, componentNew, channelsList);
                        }
                    }
                }

            }

            agentComponent.setChannels(channelsList);

            List<Sink> channelsSinks = agentComponent.getSinks();
            for(int i=0; i<channelsSinks.size(); i++){
                String nameSink = channelsSinks.get(i).getChannels();
                if(nameSink==null){
                    throw new IOException("Cannot serialize JSON, a sink isn't connected to a channel");
                }
            }
            List<Channel> channelSource = agentComponent.getChannels();
            for(int i=0; i<channelSource.size(); i++){
                String nameSource = channelSource.get(i).getSources();
                if(nameSource == null){
                    throw new IOException("Cannot serialize JSON, a channel isn't connected to a source");
                }
            }

            if(sourcesList.isEmpty() || sinksList.isEmpty() || channelsList.isEmpty()){
                throw new IOException("Cannot serialize JSON, a component is empty");
            }

        } catch (IOException e) {
            throw new IOException("Cannot serialize JSON", e);
        }catch (NumberFormatException e) {
            throw new NumberFormatException("Null Value JSON");
        }
    }

    private void connectChannelSource(List<Source> sources, String val, String componentNew, List<Channel> channels){
        boolean connect = false;
        for(int i=0; i<sources.size(); i++){
            String nameSource = sources.get(i).getId();
            if(componentNew.equals(nameSource)){

                connect = checkChannelConnection(channels, val, nameSource);

                if(connect){
                    sources.get(i).setChannels(val);
                }
            }
        }
    }

    private void connectChannelSink(List<Sink> sinks, String val, String componentNew, List<Channel> channels){
        boolean connect = false;
        for(int i=0; i<sinks.size(); i++){
            String nameSink = sinks.get(i).getId();
            if(componentNew.equals(nameSink)){

                connect = checkChannelConnection(channels, val, nameSink);

                if(connect){
                    sinks.get(i).setChannels(val);
                }
            }
        }
    }

    private boolean checkChannelConnection(List<Channel> channels, String val, String name){

        boolean connect = false;
        for(int j=0; j<channels.size(); j++) {
            String ch = channels.get(j).getId();
            if(ch.equals(val)){
                connect = true;
                channels.get(j).setSources(name);
            }
        }

        return connect;
    }



}
