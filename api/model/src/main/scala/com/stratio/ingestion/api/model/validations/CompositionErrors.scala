package com.stratio.ingestion.api.model.validations

import com.stratio.ingestion.api.model.channel.AgentChannel
import com.stratio.ingestion.api.model.commons.{Entity, Agent}
import com.stratio.ingestion.api.model.sink.AgentSink
import com.stratio.ingestion.api.model.source.AgentSource

import scala.collection.mutable.ListBuffer

/**
 * Created by miguelsegura on 21/10/15.
 */
class CompositionErrors extends ModelErrors {

  /* Pending of workflow implementation*/
//  def checkWorkflowAgents(workflow : Workflow) : Unit = {
//    if(workflow.agents.==(None)){
//      writeErrorMessage(workflow);
//    }
//  }
  var listMsg = ListBuffer.empty[String]

  def checkAgentSources(agent: Agent) :  ListBuffer[String] = {
    if(agent.source.==(None)){
      listMsg = writeErrorMessage(agent, "source", listMessages);
    }
    listMsg
  }

  def checkAgentSinks(agent: Agent) :  ListBuffer[String] = {
    if(agent.sinks.==(None)){
      listMsg = writeErrorMessage(agent, "sink", listMessages);
    }
    listMsg
  }

  def checkAgentChannels(agent: Agent) :  ListBuffer[String] = {
    if(agent.channels.==(None)){
      listMsg = writeErrorMessage(agent, "channel", listMessages);
    }
    listMsg
  }

  def checkSourceSettings(source: AgentSource) :  ListBuffer[String] = {
    if(source.settings.==(None)){
      listMsg = writeErrorMessage(source, "settings", listMessages);
    }
    listMsg
  }

  def checkSinkSettings(sink: AgentSink) :  ListBuffer[String] = {
    if(sink.settings.==(None)){
      listMsg = writeErrorMessage(sink, "settings", listMessages);
    }
    listMsg
  }

  def checkChannelSettings(channel: AgentChannel) :  ListBuffer[String] = {
    if(channel.settings.==(None)){
      listMsg = writeErrorMessage(channel, "settings", listMessages);
    }
    listMsg
  }

  def checkEntityIdOrType(entity: Entity) :  ListBuffer[String] = {
    if(entity.id.==(None)){
      listMsg = writeErrorMessage(entity, "Id", listMessages)
    }
    if(entity.typo.==(None)){
      listMsg = writeErrorMessage(entity, "typo", listMessages)
    }
    listMsg
  }

  def checkSinkChannel(sink: AgentSink) :  ListBuffer[String] = {
    if(sink.channels.==(None)){
      listMsg = writeErrorMessage(sink, "channel", listMessages)
    }
    listMsg
  }

  def checkChannelSource(channel: AgentChannel) :  ListBuffer[String] = {
    if(channel.sources.==(None)){
      listMsg = writeErrorMessage(channel, "source", listMessages)
    }
    listMsg
  }

  def writeErrorMessage(agent: Agent, component: String, listMessages: ListBuffer[String]) :  ListBuffer[String] = {
    listMessages += "Agent " + agent.id + " doesn't have a " + component
  }

  def writeErrorMessage(entity: Entity, message: String, listMessages: ListBuffer[String]) :  ListBuffer[String] = {
    listMessages += "Component " + entity.id + " of type " + entity.typo + " doesn't have " + message
  }

}
