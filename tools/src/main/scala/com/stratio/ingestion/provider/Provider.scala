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
package com.stratio.ingestion.provider

object Provider extends ZkProviderComponent {

  val providerConfigName = "provider"
  val repositoryField = "repository"
  val pathField = "path"
  val overwriteField = "overwrite"
  val defaultRepositoryType = "zookeeper"
  val defaultPath = "/tmp/workflow"
  val defaultOverwrite = "false"

  def downloadFiles(workflowId: String) =
    repositoryType match {
      case "zookeeper" => zkProvider.downloadFiles(workflowId, path, overwrite.toBoolean)
      case repo => println(s"Repository $repo not supported")
    }

  def uploadFiles(workflowId: String) =
    println("Upload files is not supported yet")

  private def repositoryType =
    getFromProviderConfig(repositoryField, defaultRepositoryType)
  
  private def path =
    getFromProviderConfig(pathField, defaultPath)

  private def overwrite =
    getFromProviderConfig(overwriteField, defaultOverwrite)

  private def getFromProviderConfig(field: String, default: String): String =
    config.getConfig(providerConfigName).flatMap(_.getString(field))
      .getOrElse(default)

}
