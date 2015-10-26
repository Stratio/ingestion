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
import com.stratio.ingestion.api.model.commons.{Attribute, Entity, Agent}
import com.stratio.ingestion.api.model.sink.AgentSink

import scala.collection.mutable.ListBuffer

/**
 * Created by miguelsegura on 21/10/15.
 */
class ConnectionErrors extends ModelErrors {

  var listMsg = ListBuffer.empty[String]
  var listChannels = List.empty[AgentChannel]

  def checkSinkIsConnected(agent: Agent) : ListBuffer[String] = {
    agent.sinks
      .filter(_.channels == None.orNull)
      .foreach(sink => listMsg = writeErrorMessage(sink, "channel", listMessages))

    listMessages
  }

  def checkSinkIsConnectedWithChannelThatExists(agent: Agent) : ListBuffer[String] = {
    listChannels = agent.channels.toList

    agent.sinks
      .filter(_.channels == None.orNull)
      .foreach {
      sink =>
        if (!listChannels.contains(sink.channels))
          listMsg = writeErrorMessage(sink, "channe that exists", listMessages)
      }
    listMessages
  }


  def checkChannelIsConnected(agent: Agent) : ListBuffer[String] = {
    agent.channels
      .filter(_.source.equals(None))
      .foreach(channel => listMsg = writeErrorMessage(channel, "source", listMessages))

    listMessages
  }

  def checkChannelIsConnectedWithSourceThatExists(agent: Agent) : ListBuffer[String] = {
    agent.channels
      .filter(!_.source.equals(None))
      .foreach {
      channel =>
        if(!agent.source.equals(channel.source))
          listMsg = writeErrorMessage(channel, "source that exists", listMessages)
    }

    listMessages
  }

  def writeErrorMessage(agent: Agent, component: String, listMessages: ListBuffer[String]) :  ListBuffer[String] = {
    listMessages += "Agent " + agent.id + " doesn't have a " + component
  }

  def writeErrorMessage(entity: Entity, message: String, listMessages: ListBuffer[String]) : ListBuffer[String] = {
    listMessages += "Component " + entity.name + " of type " + entity._type + " doesn't have " + message
  }

  def writeErrorMessage(entity: Entity, setting: Attribute, message: String, listMessages: ListBuffer[String]) :
  ListBuffer[String] = {
    listMessages += "Component " + entity.name + " of type " + entity._type + " doesn't have " +
      message + " " + setting.name
  }

}

object connectionErrors {

  val connect = new ConnectionErrors();
  var listMsg = ListBuffer.empty[String
]
  def noChannels(agent: Agent) : ListBuffer[String] = {
    val agentFlume : Agent = agent
    listMsg = connect.checkSinkIsConnected(agentFlume);
    listMsg = connect.checkChannelIsConnected(agentFlume);

    listMsg
  }

  def notConnectedThatExists(agent: Agent) : ListBuffer[String] = {
    val agentFlume : Agent = agent
    listMsg = connect.checkSinkIsConnectedWithChannelThatExists(agentFlume);
    listMsg = connect.checkChannelIsConnectedWithSourceThatExists(agentFlume);

    listMsg
  }

}