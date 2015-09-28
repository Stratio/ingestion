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

import java.util.ArrayList;
import java.util.List;

import com.stratio.ingestion.model.channel.Channel;
import com.stratio.ingestion.model.sink.Sink;
import com.stratio.ingestion.model.source.Source;

/**
 * Created by miguelsegura on 18/09/15.
 */
public class AgentComponent {

    private List<Source> sources = new ArrayList<>();
    private List<Sink> sinks = new ArrayList<>();
    private List<Channel> channels = new ArrayList<>();

    public AgentComponent(){}

    public String formatString(String inputString) {

        inputString = inputString.replace("\"", "");
        inputString = inputString.replace("[", "");
        inputString = inputString.replace("]", "");
        //        inputString = inputString.replace(",", " ");

        return inputString;
    }

//    public void addSource(Source source){
//        sources.add(source);
//    }
//
//    public void addSink(Sink sink){
//
//        sinks.add(sink);
//    }
//
//    public void addChannel(Channel channel){
//
//        channels.add(channel);
//    }

    public List<Source> getSources() {
        return sources;
    }

    public void setSources(List<Source> sources) {
        this.sources = sources;
    }

    public List<Sink> getSinks() {
        return sinks;
    }

    public void setSinks(List<Sink> sinks) {
        this.sinks = sinks;
    }

    public List<Channel> getChannels() {
        return channels;
    }

    public void setChannels(List<Channel> channels) {
        this.channels = channels;
    }
}
