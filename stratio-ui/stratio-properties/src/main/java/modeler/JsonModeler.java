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
 * Created by miguelsegura on 18/09/15.
 */

package modeler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.ingestion.model.AgentComponent;
import com.stratio.ingestion.model.Attribute;
import com.stratio.ingestion.model.channel.Channel;
import com.stratio.ingestion.model.sink.Sink;
import com.stratio.ingestion.model.source.Source;


public class JsonModeler {

    private static final Logger log = LoggerFactory.getLogger(JsonModeler.class);

    private String element;
    private AgentComponent agent = new AgentComponent();

    public JsonModeler(String jsonFile) throws IOException {

        try {

            ObjectMapper mapper = new ObjectMapper();

            BufferedReader fileReader = new BufferedReader(
                    new FileReader(jsonFile));

            JsonNode rootNode = mapper.readValue(fileReader, JsonNode.class);

            /*** Create Sources ***/
            element = "sources";
            agent = getAttributesComponents(rootNode, element, agent);

            /*** Create Sinks ***/
            element = "sinks";
            agent = getAttributesComponents(rootNode, element, agent);

            /*** Create Channels ***/
            element = "channels";
            agent = getAttributesComponents(rootNode, element, agent);

        } catch (Exception e) {
            throw new IOException("Cannot serialize JSON", e);
        }
    }


    private AgentComponent getAttributesComponents(JsonNode rootNode, String elements, AgentComponent component) throws
            IOException {

        JsonNode sourcesNode = rootNode.path(elements);

        JsonNode componentsNode = sourcesNode.path("components");
        Iterator<JsonNode> compIte = componentsNode.getElements();

        List<Source> sourcesList = new ArrayList<>();
        List<Sink> sinkList = new ArrayList<>();
        List<Channel> channelList = new ArrayList<>();

        while (compIte.hasNext()) {
            JsonNode compNext = compIte.next();

            JsonNode id = compNext.path("id");
            JsonNode type = compNext.path("type");
            JsonNode name = compNext.path("name");
            JsonNode description = compNext.path("description");

            String channelsString = "";
            String sourcesString = "";

            if(elements.equals("sources") || elements.equals("sinks")){
                JsonNode channels = compNext.path("channels");
                if(channels.isNull()){
                    throw new IOException("Cannot serialize JSON,"+ id+ "it isn't connected to a channel");
                }
                channelsString = component.formatString(channels.asText());
            } else {
                JsonNode sources = compNext.path("sources");
                if(sources.isNull()){
                    throw new IOException("Cannot serialize JSON,"+ id+ "it isn't connected to a source");
                }
                sourcesString = component.formatString(sources.asText());
            }


            String idString = component.formatString(id.asText());
            String typeString = component.formatString(type.asText());
            String nameString = component.formatString(name.asText());
            String descripString = component.formatString(description.asText());




            List<Attribute> attributesList = new ArrayList<>();
            List<JsonNode> listSettings = compNext.findValues("settings");
            JsonNode settingsValues = listSettings.get(0);
            Iterator<JsonNode> settingsNodeElem = settingsValues.getElements();
            JsonNode settingsNode = settingsNodeElem.next();
            Iterator<Map.Entry<String, JsonNode>> listValues = settingsNode.getFields();
            while(listValues.hasNext()) {
                Map.Entry<String, JsonNode> setting = listValues.next();
                String key = setting.getKey();
                JsonNode values = setting.getValue();
                Iterator<JsonNode> attribValues = values.getElements();

                Attribute attribute = new Attribute();
                attribute.setId(key);

                while(attribValues.hasNext()){
                    JsonNode settingField = attribValues.next();
                    type = settingField.path("type");
                    String settingType = component.formatString(type.asText());
                    name = settingField.path("name");
                    String settingName = component.formatString(name.asText());
                    description = settingField.path("description");
                    String settingDescrip = component.formatString(description.asText());
                    JsonNode required = settingField.path("required");
                    String settingReq = component.formatString(required.asText());
                    JsonNode value = settingField.path("value");

                    attribute.setType(settingType);
                    attribute.setName(settingName);
                    attribute.setDescription(settingDescrip);

                    if(settingReq.equals("true")){
                        attribute.setRequired(true);
                    } else {
                        attribute.setRequired(false);
                    }

                    if(settingType.equals("string") && value.isTextual()){
                        String valueString = component.formatString(value.asText());
                        attribute.setValueString(valueString);
                    }

                    if(settingType.equals("integer") && value.isInt()){
                        Integer valueInteger = value.getIntValue();
                        attribute.setValueInteger(valueInteger);
                    }

                    if(settingType.equals("boolean") && value.isBoolean()){
                        boolean valueBoolean = value.getBooleanValue();
                        attribute.setValueBoolean(valueBoolean);
                    }

                    attributesList.add(attribute);

                }
            }

            if(elements.equals("sources")){
                Source source = new Source();
                source.setId(idString);
                source.setType(typeString);
                source.setName(nameString);
                source.setDescription(descripString);
                source.setChannels(channelsString);
                source.setSettings(attributesList);
                sourcesList.add(source);
                component.setSources(sourcesList);

            }

            if(elements.equals("sinks")){
                Sink sink = new Sink();
                sink.setId(idString);
                sink.setType(typeString);
                sink.setName(nameString);
                sink.setDescription(descripString);
                sink.setChannels(channelsString);
                sink.setSettings(attributesList);
                sinkList.add(sink);
                component.setSinks(sinkList);
            }

            if(elements.equals("channels")){
                Channel channel = new Channel();
                channel.setId(idString);
                channel.setType(typeString);
                channel.setName(nameString);
                channel.setDescription(descripString);
                channel.setSources(sourcesString);
                channel.setSettings(attributesList);
                channelList.add(channel);
                component.setChannels(channelList);
            }
        }


        return component;
    }


    public AgentComponent getAgent() {
        return agent;
    }

    public void setAgent(AgentComponent agent) {
        this.agent = agent;
    }

}


