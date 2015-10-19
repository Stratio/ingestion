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

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait WorkflowServiceComponentTest extends WorkflowServiceComponent{

	val workflowService: WorkflowService = new WorkflowServiceDummy{}

	trait WorkflowServiceDummy extends WorkflowService {

		def getAll: Future[Seq[Workflow]] =
			Future {
				Seq(dummyWorkflow1, dummyWorkflow2)
			}

		def create(workflow: Workflow): Future[Workflow] =
			Future {
				workflow
			}

		def update(workflow: Workflow): Future[Workflow] =
			Future {
				workflow
			}

		def get(id: String): Future[Workflow] =
			Future {
				dummyWorkflow1
			}

		def delete(id: String): Future[Workflow] =
			Future {
				dummyWorkflow1
			}

	}
}