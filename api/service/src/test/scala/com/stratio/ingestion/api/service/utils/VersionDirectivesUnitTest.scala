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
package com.stratio.ingestion.api.service.utils

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import spray.testkit.ScalatestRouteTest
import spray.routing.HttpService
import spray.http.StatusCodes._

@RunWith(classOf[JUnitRunner])
class VersionDirectivesUnitTest extends WordSpec
with Matchers
with BeforeAndAfterEach
with ScalatestRouteTest
with HttpService
with VersionDirectives {

  def actorRefFactory = system

  val defaultVersion = 1

  lazy val dummyRoute =
    pathPrefix(Version) {
      case 1 =>
        complete("v1")
      case apiVersion =>
        complete((NotFound, s"Version $apiVersion not found"))
    } ~ complete("V1 by default")

  "The vestion directive" when {
    "receive a version" should {
      "return the version number" in {
        Get("/v1/") ~> dummyRoute ~> check {
          status should equal(OK)
          responseAs[String] === "v1"
        }
      }
      "return 404 when the version is not supported" in {
        Get("/v2/") ~> dummyRoute ~> check {
          status should equal(NotFound)
        }
      }
    }

    "doesn't receive a version" should {
      "return a version by default" in {
        Get(s"/") ~> dummyRoute ~> check {
          status should equal(OK)
          responseAs[String] === "V1 by default"
        }
      }
    }
  }
}