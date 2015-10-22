package com.stratio.ingestion.api.model.validations

import com.stratio.ingestion.api.model.commons.{Agent, Entity}
import com.stratio.ingestion.api.model.source.AgentSource

import scala.collection.mutable.ListBuffer

/**
 * Created by miguelsegura on 21/10/15.
 */
trait ModelErrors{
  val listMessages = ListBuffer.empty[String]

  def writeErrorMessage(agent: Agent, component: String, listMessages: ListBuffer[String]) :  ListBuffer[String]
  def writeErrorMessage(entity: Entity, message: String, listMessages: ListBuffer[String]) :  ListBuffer[String]

}
