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

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import spray.testkit.ScalatestRouteTest
import spray.routing.HttpService
import spray.http.StatusCodes._
import spray.httpx.SprayJsonSupport._

@RunWith(classOf[JUnitRunner])
class WorkflowRouteUnitTest extends WordSpec
with Matchers
with BeforeAndAfterEach
with ScalatestRouteTest
with HttpService
with WorkflowRoute
with WorkflowServiceComponentTest {

  def actorRefFactory = system
  val rootPath = "workflows"

  import WorkflowParser._

  "The workflow route" when {
    "GET workflow" should {

      val workflowId = 1

      "return a workflow" in {
        Get(s"/$rootPath/$workflowId") ~> workflowRoute ~> check {
          status should equal (OK)
          responseAs[Workflow] === dummyWorkflow1
        }
      }
      "return 404 when the workflow is not found" ignore {
        Get(s"/$rootPath/$workflowId") ~> workflowRoute ~> check {
          status should equal (NotFound)
        }
      }
    }

    "GET all workflows" should {
      "return all workflows" in {
        Get(s"/$rootPath/") ~> workflowRoute ~> check {
          status should equal (OK)
          responseAs[Seq[Workflow]] === Seq(dummyWorkflow1, dummyWorkflow2)
        }
      }
      "return empty list when there isn't any workflow" ignore {
        Get(s"/$rootPath/") ~> workflowRoute ~> check {
          status should equal (OK)
          responseAs[Seq[Workflow]] === Seq()
        }
      }
    }

    "POST workflow" should {
      "create a workflow" in {
        Post(s"/$rootPath/", dummyWorkflow1) ~> workflowRoute ~> check {
          status should equal (OK)
          responseAs[Workflow] === dummyWorkflow1
        }
      }
    }

    "PUT workflow" should {
      "update a workflow" in {
        Put(s"/$rootPath/", dummyWorkflow1) ~> workflowRoute ~> check {
          status should equal (OK)
          responseAs[Workflow] === dummyWorkflow1
        }
      }
    }

    "DELETE workflow" should {

      val workflowId = 1

      "remove a workflow" in {
        Delete(s"/$rootPath/$workflowId") ~> workflowRoute ~> check {
          status should equal (OK)
          responseAs[Workflow] === dummyWorkflow1
        }
      }
      "return 404 when the workflow is not found" ignore {
        Delete(s"/$rootPath/$workflowId") ~> workflowRoute ~> check {
          status should equal (NotFound)
        }
      }
    }
  }
}