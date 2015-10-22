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
package com.stratio.ingestion.api.core.dao

import com.stratio.ingestion.api.core.utils.Constants
import com.stratio.ingestion.api.model.commons.WorkFlow
import com.typesafe.scalalogging.LazyLogging
import org.apache.curator.framework.CuratorFramework

import scala.pickling.Defaults._
import scala.pickling.binary._


/**
 * Created by aitor on 10/16/15.
 */
case class ZookeeperRepositoryDaoImpl(template: CuratorFramework) extends RepositoryDao
  with LazyLogging  {

  val curatorZookeeperClient= template

  val dto: ZookeeperDTO= ZookeeperDTO.initialize(curatorZookeeperClient)

  override def createWorkflow(workflow: WorkFlow): Boolean = {
    try {
      dto.create(Constants.ZOO_WORKFLOWS_PATH + "/" + workflow.id , workflow.pickle.value)

    } catch {

      case ex: IllegalStateException => {
        logger.error("Unable to connect to repository: " + ex.getMessage)
        false
      }
      case ex: Exception => {
        logger.error("Undefined Exception: " + ex.getStackTrace)
        false
      }
    }
    true
  }

  override def getWorkflow(id: String): WorkFlow = {
    val element= dto.getElementData(Constants.ZOO_WORKFLOWS_PATH + "/" + id)
    element.unpickle[WorkFlow]
  }

  override def deleteWorkflow(id: String): Boolean = {
    try {
      dto.delete(Constants.ZOO_WORKFLOWS_PATH + "/" + id)
    } catch {

      case ex: IllegalStateException => {
        logger.error("Unable to connect to repository: " + ex.getMessage)
        false
      }
      case ex: Exception => {
        logger.error("Undefined Exception: " + ex.getStackTrace)
        false
      }
    }
    true
  }

  override def listAll(): Boolean = {
    true
  }

  override def addElementToWorkflow(): Boolean = ???




  override def getWorkflowElements(): Boolean = ???

  override def deleteElement(): Boolean = ???

  override def updateElement(): Boolean = ???
}
