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
import com.stratio.ingestion.api.model.source.AgentSource

import scala.collection.mutable.ListBuffer

/**
 * Created by miguelsegura on 21/10/15.
 */
class ValueErrors extends ModelErrors {

  var listMsg = ListBuffer.empty[String]


  def checkSourceRequiredIsFilled(source: AgentSource) :  ListBuffer[String] = {
    if(!source.settings.isEmpty){
      source.settings
        .filter(_.required.equals(true))
        .filter(_.value.isEmpty)
        .foreach(setting => listMsg = writeErrorMessage(source, setting, "a required value filled", listMessages))

    }
    listMsg
  }

  def checkSinkRequiredIsFilled(sink: AgentSink) :  ListBuffer[String] = {
    if(!sink.settings.isEmpty){
      sink.settings
        .filter(_.required.equals(true))
        .filter(_.value.isEmpty)
        .foreach(setting => listMsg = writeErrorMessage(sink, setting, "a required value filled", listMessages))
    }
    listMsg
  }

  def checkChannelRequiredIsFilled(channel: AgentChannel) : ListBuffer[String] = {
    if(!channel.settings.isEmpty) {
      channel.settings
        .filter(_.required.equals(true))
        .filter(_.value.isEmpty)
        .foreach(setting => listMsg = writeErrorMessage(channel, setting, "required value filled", listMessages))
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

object valueErrors {

  val connect = new ValueErrors();
  var listMsg = ListBuffer.empty[String]
  def settingFailure(agent: Agent) : ListBuffer[String] = {

    listMsg = connect.checkSourceRequiredIsFilled(agent.source);
    agent.sinks.foreach(sink =>listMsg = connect.checkSinkRequiredIsFilled(sink))
    agent.channels.foreach(channel =>listMsg = connect.checkChannelRequiredIsFilled(channel))

    listMsg
  }

}