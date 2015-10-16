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

import spray.routing._
import spray.httpx.SprayJsonSupport._
import WorkflowParser._

trait WorkflowRoute extends HttpService {
	self: WorkflowServiceComponent =>

  implicit def executionContext = actorRefFactory.dispatcher

	lazy val workflowRoute =
		pathPrefix("workflows") {
			pathEndOrSingleSlash {
				get {
					getAllWorkflows
				} ~
        (post & entity(as[Workflow])){ workflow =>
					createWorkflow(workflow)
				} ~
        (put & entity(as[Workflow])){ workflow =>
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

	def getAllWorkflows	=
		complete {
      workflowService.getAll
    }

	def createWorkflow(workflow: Workflow)	=
    complete {
      workflowService.create(workflow)
    }

	def updateWorkflow(workflow: Workflow) =
    complete {
      workflowService.update(workflow)
    }

	def getWorkflow(id: String)	=
    complete {
      workflowService.get(id)
    }

	def deleteWorkflow(id: String) =
    complete {
      workflowService.delete(id)
    }
}
