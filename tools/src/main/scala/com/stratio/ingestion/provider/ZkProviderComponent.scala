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

import java.io.{FileOutputStream, File}

import com.stratio.common.utils.components.config.impl.TypesafeConfigComponent
import com.stratio.common.utils.components.logger.impl.Slf4jLoggerComponent
import com.stratio.common.utils.components.repository.impl.ZookeeperRepositoryComponent

trait ZkProviderComponent extends ZookeeperRepositoryComponent
  with TypesafeConfigComponent
  with Slf4jLoggerComponent{

  val zkProvider = new ZkProvider {}

  trait ZkProvider {

    val parentPath: String = "stratio/workflow/workflows/"

    def importFiles(workflowId: String, path: String) = {
      for {
        filename <- repository.getNodes(parentPath + workflowId)
        element <- repository.get(parentPath + workflowId, filename)
      } yield {
        val folder = new File(s"$path/$workflowId")
        folder.mkdirs()
        val f = new File(s"$path/$workflowId/$filename")
        f.createNewFile()

        val fos = new FileOutputStream(s"$path/$workflowId/$filename")
        fos.write(element)
        fos.close()

        println(s"File $filename stored.")
      }
    }

    def exportFiles(workflowId: String, path: String) =
      println("Export files is not supported yet")
  }
}