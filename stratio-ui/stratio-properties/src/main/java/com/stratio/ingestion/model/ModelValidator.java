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
package com.stratio.ingestion.model;

import java.util.List;

import com.stratio.ingestion.model.channel.Channel;
import com.stratio.ingestion.model.sink.Sink;
import com.stratio.ingestion.model.source.Source;

/**
 * Created by miguelsegura on 19/10/15.
 */
public class ModelValidator {

    public void checkChannelSource(Source source) throws Exception {
        String channels = source.getChannels();

        if(channels.equals(null) || channels.equals("")){
            throw new Exception("Can't connect sink");
        }
    }

    public void checkChannelSink(Sink sink) throws Exception {
        String channels = sink.getChannels();

        if(channels.equals(null) || channels.equals("")){
            throw new Exception("Can't connect sink");
        }
    }

    public void checkAttributes(Attribute attribute) throws Exception {
        boolean required = attribute.getRequired();

        if(required && attribute.getValueString().equals(null) && attribute.getValueInteger().equals(null) && attribute
                .getValueBoolean().equals(null)) {
            throw new Exception("Can't write attributes, required field without value");
        }
    }

    public void checkComponent(String typeComponent) throws Exception{
        if(!typeComponent.equals("sources") && !typeComponent.equals("sinks") && !typeComponent.equals
                ("channels")){
            throw new Exception("Cannot serialize JSON, incorrect component");
        }
    }

    public void checkSinkConnection(List<Sink> sinks) throws Exception {
        for(int i=0; i<sinks.size(); i++){
            String nameSink = sinks.get(i).getChannels();
            if(nameSink==null){
                throw new Exception("Cannot serialize JSON, a sink isn't connected to a channel");
            }
        }
    }

    public void checkSourceConnection(List<Source> sources) throws Exception {
        for(int i=0; i<sources.size(); i++){
            String nameChannel = sources.get(i).getChannels();
            if(nameChannel == null){
                throw new Exception("Cannot serialize JSON, a source isn't connected to a channel");
            }
        }
    }

    public void checkChannelConnection(List<Channel> channels) throws Exception {
        for(int i=0; i<channels.size(); i++){
            String nameSource = channels.get(i).getSources();
            if(nameSource == null){
                throw new Exception("Cannot serialize JSON, a channel isn't connected to a source");
            }
        }
    }
}
