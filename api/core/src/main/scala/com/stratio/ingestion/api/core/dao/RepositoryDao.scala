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

import com.stratio.ingestion.api.model.commons.WorkFlow

/**
 * Repository DAO interface
 */
trait RepositoryDao {

  /**
   * Create a Workflow entity in the repository
   *
   * @param workflow The workflow to create
   * @return Boolean True if Workflow has been created properly
   */
  def createWorkflow(workflow: WorkFlow): Boolean

  /**
   * Get a workflow from the repository
   *
   * @param id The workflow id
   * @return The workflow entity
   */
  def getWorkflow(id: String): Option[WorkFlow]

  /**
   * Checks if a workflow exists in the repository
   *
   * @param id The workflow id
   * @return Boolean True if the Workflow exists in the repository
   */
  def existsWorkflow(id: String): Boolean

  /**
   * Checks if a child element of a workflow exists in the repository
   *
   * @param workflowId The Workflow id
   * @param elementId The element id
   * @return Boolean True if the Workflow exists in the repository
   */
  def existsWorkflowElement(workflowId: String, elementId: String): Boolean

  /**
   * Get all the existing Workflows
   *
   * @return Seq[Workflow] A sequence of Workflows
   */
  def listAll(): Seq[WorkFlow]

  /**
   * Delete a Workflow of the repository
   *
   * @param id The Workflow id of Workflow to delete
   * @return Boolean True if the Workflow has been deleted
   */
  def deleteWorkflow(id: String): Boolean

  /**
   * Add one element to an existing Workflow
   *
   * @param workflowId The Workflow Id of parent Workflow
   * @param elementId  The Element Id of Element to add
   * @param content The Element content
   * @return Boolean True if the element has been created
   */
  def addElementToWorkflow(workflowId: String, elementId: String, content: String): Boolean

  /**
   * Get all the elements children of a Workflow
   *
   * @param workflowId The Workflow Id of Workflow to read the children data
   * @return Seq[String] A sequence of all the different children element
   */
  def getWorkflowElements(workflowId: String): Seq[String]


  /**
   * Get one specific element of a workflow
   *
   * @param workflowId The Workflow Id of Workflow to read the children data
   * @param elementId  The Element Id of Element to read
   * @return Seq[String] A sequence of all the different children element
   */
  def getWorkflowElement(workflowId: String, elementId: String): Option[String]

  /**
   * Update one specific element chindren of a Workflow
   *
   * @param workflowId The Workflow parent id
   * @param elementId The element id
   * @param content The content to udpate
   * @return Boolean True if the content has been updated
   */
  def updateWorkflowElement(workflowId: String, elementId: String, content: String): Boolean

  /**
   * Delete one specific element of a Workflow
   *
   * @param workflowId The Workflow Id of parent Workflow
   * @param elementId  The Element Id of Element to add
   * @return Boolean True if the content has been deleted
   */
  def deleteWorkflowElement(workflowId: String, elementId: String): Boolean


  
}