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

import java.io.IOException;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.stratio.ingestion.model.AgentComponent;
import com.stratio.ingestion.model.Attribute;
import com.stratio.ingestion.model.channel.Channel;
import com.stratio.ingestion.model.sink.Sink;
import com.stratio.ingestion.model.source.Source;

import modeler.JsonModeler;


@RunWith(JUnit4.class)
public class JsonModelerTest {

    @Test
    public void testJsonLleno() throws IOException {

        JsonModeler jsModel = new JsonModeler("src/test/resources"
                + "/jsonPropertiesCorrect.json");

    }


    @Test(expected = NullPointerException.class)
    public void testRequiredFields() throws IOException {

        JsonModeler jsModel = new JsonModeler("src/test/resources"
                + "/jsonPropertiesReqWrong.json");

        AgentComponent agent = jsModel.getAgent();

        List<Source> sourcesList = agent.getSources();
        for(Source source :sourcesList){
            checkRequiredSettings(source.getSettings());
        }

        List<Sink> sinksList = agent.getSinks();
        for(Sink sink :sinksList){
            checkRequiredSettings(sink.getSettings());
        }

        List<Channel> channelList = agent.getChannels();
        for(Channel channel :channelList){
            checkRequiredSettings(channel.getSettings());
        }
    }

    @Test(expected = NullPointerException.class)
    public void testComponentsConnected() throws IOException {

        JsonModeler jsModel = new JsonModeler("src/test/resources"
                + "/jsonPropertiesConnectWrong.json");

        AgentComponent agent = jsModel.getAgent();

        List<Source> sourcesList = agent.getSources();
        for(Source source :sourcesList){
            checkConnections(source.getId(), source.getChannels());
        }

        List<Sink> sinksList = agent.getSinks();
        for(Sink sink :sinksList){
            checkConnections(sink.getId(), sink.getChannels());
        }

        List<Channel> channelList = agent.getChannels();
        for(Channel channel :channelList){
            checkConnections(channel.getId(), channel.getSources());
        }
    }


    public void checkRequiredSettings(List<Attribute> listSettings) throws NullPointerException {
        for(Attribute atrib : listSettings){
            if(atrib.getRequired() && (atrib.getValueInteger().equals(null) && atrib.getValueString().equals(null) &&
                    atrib.getValueBoolean().equals(null))){
                throw new NullPointerException("Fields not correct");
            }
        }
    }

    public void checkConnections(String component, String componentConnect) throws NullPointerException {

        if(componentConnect.equals(null) || componentConnect.equals("")){
            throw new NullPointerException("Component "+component+" not connected");
        }

    }

}


