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
    channel.settings
      .filter(_.required.equals(true))
      .filter(_.value.isEmpty)
      .foreach(setting => listMsg = writeErrorMessage(channel, setting, "required value filled", listMessages))
    listMsg
  }



  def writeErrorMessage(agent: Agent, component: String, listMessages: ListBuffer[String]) :  ListBuffer[String] = {
    listMessages += "Agent " + agent.id + " doesn't have a " + component
  }

  def writeErrorMessage(entity: Entity, message: String, listMessages: ListBuffer[String]) :  ListBuffer[String] = {
    listMessages += "Component " + entity.name + " of type " + entity.typo + " doesn't have " + message
  }

  def writeErrorMessage(entity: Entity, setting: Attribute, message: String, listMessages: ListBuffer[String]) :
  ListBuffer[String] = {
    listMessages += "Component " + entity.name + " of type " + entity.typo + " doesn't have " +
      message + " " + setting
  }
}

object valueErrors {

  val connect = new ValueErrors();
  var listMsg = ListBuffer.empty[String]
  def settingFailure(sink: AgentSink) : ListBuffer[String] = {
    val agentSink : AgentSink = sink
    listMsg = connect.checkSourceRequiredIsFilled(agentSink);

    listMsg
  }

}