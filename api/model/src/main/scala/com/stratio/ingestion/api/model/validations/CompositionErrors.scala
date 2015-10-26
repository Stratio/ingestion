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
package com.stratio.ingestion.api.model.validations

import com.stratio.ingestion.api.model.channel.AgentChannel
import com.stratio.ingestion.api.model.commons.{WorkFlow, Attribute, Entity, Agent}
import com.stratio.ingestion.api.model.sink.AgentSink
import com.stratio.ingestion.api.model.source.AgentSource

import scala.collection.mutable.ListBuffer

/**
 * Created by miguelsegura on 21/10/15.
 */
class CompositionErrors extends ModelErrors {

  var listMsg = ListBuffer.empty[String]

  def checkAgentSources(agent: Agent) :  ListBuffer[String] = {
    if(agent.source == None.orNull){
      listMsg = writeErrorMessage(agent, "source", listMessages);
    }
    listMsg
  }

  def checkAgentSinks(agent: Agent) :  ListBuffer[String] = {
    if(agent.sinks.isEmpty){
      listMsg = writeErrorMessage(agent, "sink", listMessages);
    }
    listMsg
  }

  def checkAgentChannels(agent: Agent) :  ListBuffer[String] = {
    if(agent.channels.isEmpty){
      listMsg = writeErrorMessage(agent, "channel", listMessages);
    }
    listMsg
  }

  def checkSourceSettings(source: AgentSource) :  ListBuffer[String] = {
    if(source.settings.isEmpty){
      listMsg = writeErrorMessage(source, "settings", listMessages);
    }
    listMsg
  }

  def checkSinkSettings(sink: AgentSink) :  ListBuffer[String] = {
    if(sink.settings.isEmpty){
      listMsg = writeErrorMessage(sink, "settings", listMessages);
    }
    listMsg
  }

  def checkChannelSettings(channel: AgentChannel) :  ListBuffer[String] = {
    if(channel.settings.isEmpty){
      listMsg = writeErrorMessage(channel, "settings", listMessages);
    }
    listMsg
  }

  def checkEntityIdOrType(entity: Entity) :  ListBuffer[String] = {
    if(entity.id.isEmpty || entity.id.equals("")){
      listMsg = writeErrorMessage(entity, "Id", listMessages)
    }
    if(entity._type.isEmpty || entity.id.equals("")){
      listMsg = writeErrorMessage(entity, "typo", listMessages)
    }
    listMsg
  }

  def checkSinkChannel(sink: AgentSink) :  ListBuffer[String] = {
    if(sink.channels == None.orNull){
      listMsg = writeErrorMessage(sink, "channel", listMessages)
    }
    listMsg
  }

  def checkChannelSource(channel: AgentChannel) :  ListBuffer[String] = {
    if(channel.source == None.orNull){
      listMsg = writeErrorMessage(channel, "source", listMessages)
    }
    listMsg
  }

  def writeErrorMessage(agent: Agent, component: String, listMessages: ListBuffer[String]) :  ListBuffer[String] = {
    listMessages += "Agent " + agent.id + " doesn't have a " + component
  }

  def writeErrorMessage(entity: Entity, message: String, listMessages: ListBuffer[String]) :  ListBuffer[String] = {
    listMessages += "Component " + entity.name + " of type " + entity._type + " doesn't have " + message
  }

  def writeErrorMessage(entity: Entity, setting: Attribute, message: String, listMessages: ListBuffer[String]) :
  ListBuffer[String] = {
    listMessages += "Component " + entity.name + " of type " + entity._type + " doesn't have " +
      message + " " + setting.name
  }
}

object compositionErrors {

  val comp = new CompositionErrors();
  var listMsg = ListBuffer.empty[String]

  def badComponents(agent: Agent) : ListBuffer[String] = {
    val agentFlume : Agent = agent
    listMsg = comp.checkAgentSources(agentFlume);
    listMsg = comp.checkAgentSinks(agentFlume);
    listMsg = comp.checkAgentChannels(agentFlume);

    listMsg
  }

  def badSettings(agent: Agent) : ListBuffer[String] = {
    val agentFlume : Agent = agent
    listMsg = comp.checkSourceSettings(agentFlume.source);
    agentFlume.sinks.foreach(sink =>listMsg = comp.checkSinkSettings(sink))
    agentFlume.channels.foreach(channel =>listMsg = comp.checkChannelSettings(channel))

    listMsg
  }

  def badEntity(agent: Agent) : ListBuffer[String] = {
    val agentFlume : Agent = agent
    listMsg = comp.checkEntityIdOrType(agentFlume.source);
    agentFlume.sinks.foreach(sink =>listMsg = comp.checkEntityIdOrType(sink))
    agentFlume.channels.foreach(channel =>listMsg = comp.checkEntityIdOrType(channel))

    listMsg
  }


}