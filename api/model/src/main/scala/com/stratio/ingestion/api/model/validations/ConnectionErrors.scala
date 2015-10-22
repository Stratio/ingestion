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
//var listChannels = Seq.empty[AgentChannel]
  def checkSinkIsConnected(agent: Agent) : ListBuffer[String] = {
    agent.sinks
      .filter(_.channels.isEmpty)
      .foreach(sink => listMsg = writeErrorMessage(sink, "channel", listMessages))

    listMessages
  }

  //TODO
  //Check if a sink is connected with a channel that exists in channel's list of the agent
  def checkSinkIsConnectedWithChannel(agent: Agent) : ListBuffer[String] = {
    agent.sinks
      .filter(!_.channels.isEmpty)
      .foreach{sink => listChannels = sink.channels.seq.toList;println(listChannels)}


    listMessages
  }

//  def checkChannelIsConnected(agent: Agent) : ListBuffer[String] = {
//    agent.channels
//      .filter(_.sources.isEmpty)
//      .foreach(channel => listMsg = writeErrorMessage(channel, "source", listMessages))
//
//    listMessages
//  }

  def writeErrorMessage(agent: Agent, component: String, listMessages: ListBuffer[String]) :  ListBuffer[String] = {
    listMessages += "Agent " + agent.id + " doesn't have a " + component
  }

  def writeErrorMessage(entity: Entity, message: String, listMessages: ListBuffer[String]) : ListBuffer[String] = {
    listMessages += "Component " + entity.name + " of type " + entity.typo + " doesn't have " + message
  }

  def writeErrorMessage(entity: Entity, setting: Attribute, message: String, listMessages: ListBuffer[String]) :
  ListBuffer[String] = {
    listMessages += "Component " + entity.name + " of type " + entity.typo + " doesn't have " +
      message + " " + setting
  }

}

object errors {

  val connect = new ConnectionErrors();
  var listMsg = ListBuffer.empty[String]
  def sinkNoChannels(agent: Agent) : ListBuffer[String] = {
    val agentFlume : Agent = agent
    listMsg = connect.checkSinkIsConnected(agentFlume);

    listMsg
  }

  def sinkChannelsNotConnected(agent: Agent) : ListBuffer[String] = {
    val agentFlume : Agent = agent
    listMsg = connect.checkSinkIsConnectedWithChannel(agentFlume);

    listMsg
  }

}