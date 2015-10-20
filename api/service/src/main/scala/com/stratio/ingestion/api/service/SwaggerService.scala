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
package com.stratio.ingestion.api.service

import com.gettyimages.spray.swagger.SwaggerHttpService
import com.stratio.ingestion.api.service.workflow.WorkflowRoute
import com.wordnik.swagger.model.ApiInfo
import spray.routing.HttpServiceActor
import scala.reflect.runtime.universe._

trait SwaggerService {
  self: HttpServiceActor =>

  val swaggerService = new SwaggerHttpService {
    override def apiTypes = Seq(typeOf[WorkflowRoute])

    override def apiVersion = "2.0"

    override def baseUrl = "/"

    override def docsPath = "api-docs"

    override def actorRefFactory = context

    override def apiInfo =
      Some(
        new ApiInfo(
          "Spray-Swagger Sample",
          "Ingestion API",
          "",
          "Ingestion@stratio.com",
          "Apache V2",
          "http://www.apache.org/licenses/LICENSE-2.0"
        )
      )
  }
}