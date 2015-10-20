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