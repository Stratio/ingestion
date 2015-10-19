/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.ingestion.api.service.workflow

import scala.concurrent.Future

case class Workflow(id: String,
                    name: String,
                    description: String,
                    state: String,
                    agents: Seq[String]
                     )

import spray.json._

object WorkflowParser extends DefaultJsonProtocol {

  implicit lazy val workflowFormat = jsonFormat5(Workflow)
}

trait WorkflowServiceComponent {

  val workflowService: WorkflowService

  val dummyWorkflow1 = Workflow("id-1", "name-1", "description-1", "state-1", Seq("agent1", "agent2"))
  val dummyWorkflow2 = Workflow("id-2", "name-2", "description-2", "state-2", Seq("agent1"))

  trait WorkflowService {

    def getAll: Future[Seq[Workflow]]

    def create(workflow: Workflow): Future[Workflow]

    def update(workflow: Workflow): Future[Workflow]

    def get(id: String): Future[Workflow]

    def delete(id: String): Future[Workflow]

  }

}