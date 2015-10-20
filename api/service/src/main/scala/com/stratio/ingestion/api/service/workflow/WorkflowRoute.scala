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
package com.stratio.ingestion.api.service.workflow

import com.wordnik.swagger.annotations._
import spray.routing._
import spray.httpx.SprayJsonSupport._
import WorkflowParser._

@Api(value = "/workflows", description = "Operations about workflows.", position = 0)
trait WorkflowRoute extends HttpService {
  self: WorkflowServiceComponent =>

  implicit def executionContext = actorRefFactory.dispatcher

  lazy val workflowRoute =
    pathPrefix("workflows") {
      pathEndOrSingleSlash {
        get {
          getAllWorkflows
        } ~
          (post & entity(as[Workflow])) { workflow =>
            createWorkflow(workflow)
          } ~
          (put & entity(as[Workflow])) { workflow =>
            updateWorkflow(workflow)
          }
      } ~
        path(Segment) { id =>
          get {
            getWorkflow(id)
          } ~
            delete {
              deleteWorkflow(id)
            }
        }
    }

  @ApiOperation(value = "Get all workflows",
    notes = "Get a list with all the workflows.",
    httpMethod = "GET",
    response = classOf[Workflow])
  def getAllWorkflows =
    complete {
      workflowService.getAll
    }

  @ApiOperation(value = "Create a  workflow",
    notes = "Create a new workflow.",
    httpMethod = "POST",
    response = classOf[Workflow])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workflow",
      value = "New workflow",
      dataType = "Workflow",
      required = true,
      paramType = "body")
  ))
  def createWorkflow(workflow: Workflow) =
    complete {
      workflowService.create(workflow)
    }

  @ApiOperation(value = "Update a workflow",
    notes = "Update a workflow by id.",
    httpMethod = "PUT",
    response = classOf[Workflow])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workflow",
      value = "New worflow",
      dataType = "Workflow",
      required = true,
      paramType = "body")
  ))
  def updateWorkflow(workflow: Workflow) =
    complete {
      workflowService.update(workflow)
    }

  @ApiOperation(value = "Get a workflows",
    notes = "Get a workflow by id.",
    httpMethod = "GET",
    response = classOf[Workflow])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workflowId",
      value = "id of the workflow",
      dataType = "string",
      required = true,
      paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 404, message = "Workflow not found")
  ))
  def getWorkflow(id: String) =
    complete {
      workflowService.get(id)
    }

  @ApiOperation(value = "Remove a  workflow",
    notes = "Remove a workflow by id.",
    httpMethod = "DELETE",
    response = classOf[Workflow])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workflowId",
      value = "id of the workflow",
      dataType = "string",
      required = true,
      paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 404, message = "Workflow not found")
  ))
  def deleteWorkflow(id: String) =
    complete {
      workflowService.delete(id)
    }
}
