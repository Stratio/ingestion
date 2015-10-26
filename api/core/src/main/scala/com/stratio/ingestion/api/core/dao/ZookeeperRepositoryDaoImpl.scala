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

  override def getWorkflow(id: String): Option[WorkFlow] = {
    val workflow= dto.getElementData(Constants.ZOO_WORKFLOWS_PATH + "/" + id).getOrElse {return None}
    Some(workflow.unpickle[WorkFlow])
  }

  override def existsWorkflow(id: String): Boolean = {
    dto.exists(Constants.ZOO_WORKFLOWS_PATH + "/" + id)
  }

  override def existsWorkflowElement(workflowId: String, elementId: String): Boolean = {
    dto.exists(Constants.ZOO_WORKFLOWS_PATH + "/" + workflowId + "/" + elementId)
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

  override def listAll(): Seq[WorkFlow] = {
    var workflows: Seq[WorkFlow]= Seq()
    val list= dto.getChildren(Constants.ZOO_WORKFLOWS_PATH )
    var element: Array[Byte]= Array()

    for (element <- list) {
      workflows :+= element.unpickle[WorkFlow]
    }
    workflows
  }

  override def addElementToWorkflow(workflowId: String, elementId: String, content: String): Boolean = {
    try {
      if (existsWorkflow(workflowId)) {
        dto.create(Constants.ZOO_WORKFLOWS_PATH + "/" + workflowId + "/" + elementId, content.pickle.value)
      } else {
        false
      }
      true
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

  }

  override def getWorkflowElements(workflowId: String): Seq[String] = {
    var elements: Seq[String]= Seq()
    val list= dto.getChildren(Constants.ZOO_WORKFLOWS_PATH + "/" + workflowId)
    var element: Array[Byte]= Array()

    for (element <- list) {
      elements :+= element.unpickle[String]
    }
    elements
  }

  override def getWorkflowElement(workflowId: String, elementId: String): Option[String] = {
    val element= dto.getElementData(Constants.ZOO_WORKFLOWS_PATH + "/" + workflowId + "/" + elementId).
      getOrElse {return None}
    Some(element.unpickle[String])

    //Some(dto.getElementData(Constants.ZOO_WORKFLOWS_PATH + "/" + workflowId + "/" + elementId).unpickle[String])
  }

  override def updateWorkflowElement(workflowId: String, elementId: String, content: String): Boolean = {
    try {
      if (existsWorkflowElement(workflowId, elementId)) {
        dto.update(Constants.ZOO_WORKFLOWS_PATH + "/" + workflowId + "/" + elementId, content.pickle.value)
      } else {
        false
      }
      true
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

  }

  override def deleteWorkflowElement(workflowId: String, elementId: String): Boolean = {
    try {
      dto.delete(Constants.ZOO_WORKFLOWS_PATH + "/" + workflowId + "/" + elementId)
      false
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


}
