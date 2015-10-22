package com.stratio.ingestion.api.model.validations

import com.stratio.ingestion.api.model.commons.{Entity, Agent}
import com.stratio.ingestion.api.model.sink.AgentSink

import scala.collection.mutable.ListBuffer

/**
 * Created by miguelsegura on 21/10/15.
 */
class ConnectionErrors extends ModelErrors {

//  var sinks : Seq[AgentSink]
//  val sink : AgentSink
var listMsg = ListBuffer.empty[String]

  def checkSinkIsConnected(agent: Agent) : ListBuffer[String] = {
    //    val sinks : Seq[AgentSink] = agent.sinks.seq
    val sinks = agent.sinks.seq
    //    sinks.foreach()
    val list: List[AgentSink] = sinks.toList
//    for (sink <- list) {
//      if (sink.channels.==(None)) writeErrorMessage(sink, "source");
//      println(sink)
//    }
//
//    agent.sinks
//      .filter(_.channels.isEmpty )
//      .foreach{
//      sink  => println(sink)
//    }


    agent.sinks
      .filter(_.channels.isEmpty)
      .foreach(sink => listMsg = writeErrorMessage(sink, "source", listMessages))

    listMessages
//    if(){
//
//    }
  }

  def writeErrorMessage(agent: Agent, component: String, listMessages: ListBuffer[String]) :  ListBuffer[String] = {
    listMessages += "Agent " + agent.id + " doesn't have a " + component
  }

  def writeErrorMessage(entity: Entity, message: String, listMessages: ListBuffer[String]) : ListBuffer[String] = {
    listMessages += "Component " + entity.id + " of type " + entity.typo + " doesn't have " + message
  }

}

object errors {

  val connect = new ConnectionErrors();
  var listMsg = ListBuffer.empty[String]
  def giveMeAgent(agent: Agent) {
    val agentFlume : Agent = agent
    listMsg = connect.checkSinkIsConnected(agentFlume);
//    agentFlume = agent
    println(listMsg)
  }


}