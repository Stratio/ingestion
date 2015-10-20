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
package com.stratio.ingestion.api.core.dao

import org.apache.curator.framework.CuratorFramework


/**
 * Created by aitor on 10/16/15.
 */
case class ZookeeperRepositoryDaoImpl(template: CuratorFramework) extends RepositoryDao {

  private val curatorZookeeperClient= template

  private val dto: ZookeeperDTO= ZookeeperDTO(curatorZookeeperClient)

  override def createWorkflow(): Boolean = {
    //val contents: Array[Byte]= _
    //dto.create(Constants.ZOO_WORKFLOWS_PATH + "/1234" , contents)
    true
  }

  override def listAll(path: String): Boolean = {
    true
  }

  override def addElementToWorkflow(): Boolean = ???

  override def deleteWorkflow(): Boolean = ???

  override def getWorkflow(): Boolean = ???

  override def getWorkflowElements(): Boolean = ???

  override def deleteElement(): Boolean = ???

  override def updateElement(): Boolean = ???
}
