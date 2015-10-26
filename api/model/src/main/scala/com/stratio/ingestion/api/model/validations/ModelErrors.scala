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

import com.stratio.ingestion.api.model.commons.{Attribute, Agent, Entity}
import com.stratio.ingestion.api.model.source.AgentSource

import scala.collection.mutable.ListBuffer

/**
 * Created by miguelsegura on 21/10/15.
 */
trait ModelErrors{
  val listMessages = ListBuffer.empty[String]

  def writeErrorMessage(agent: Agent, component: String, listMessages: ListBuffer[String]) :  ListBuffer[String]
  def writeErrorMessage(entity: Entity, message: String, listMessages: ListBuffer[String]) :  ListBuffer[String]
  def writeErrorMessage(entity: Entity, setting: Attribute, message: String, listMessages: ListBuffer[String]) :
  ListBuffer[String]

}
