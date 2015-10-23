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

import com.stratio.ingestion.api.core.utils._
import com.stratio.ingestion.api.model.commons.{Agent, WorkFlow}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.log4j._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, _}

/**
 * Created by aitor on 10/19/15.
 */
@RunWith(classOf[JUnitRunner])
class ZookeeperIT  extends WordSpec
  with LazyLogging
  with ShouldMatchers
  with MockitoSugar
  with BeforeAndAfterAll {

  var curator: CuratorFramework= _
  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  var conf: Config= _
  var dao: ZookeeperRepositoryDaoImpl= _


  "The ZookeeperRepositoryDaoImpl" when {

    "call the createWorkflow method" should {
      "create a new workflow in Zookeeper cluster" in {

        val myId= "1234"

        val agents: Seq[Agent]= Seq()
        val workflow= WorkFlow.apply(myId, "my workflow", "desc", agents)

        assert(dao.createWorkflow(workflow))
        assert(dao.getWorkflow(myId).get.name == "my workflow")
        assert(dao.deleteWorkflow(myId))
      }
    }


    "call the getWorkflow method" should {
      "get an existing workflow from Zookeeper" in {

        val myId= "1235"

        val a= Seq.empty[String]
        val agents= Seq.empty[String]
        val workflow= WorkFlow(myId, "my workflow", "desc", Seq())

        dao.createWorkflow(workflow)

        val result= dao.getWorkflow(myId)
        assert(dao.deleteWorkflow(myId))

        assertResult("my workflow") {
          result.get.name
        }
      }
    }

    "call the getWorkflow method" should {
      "get a Non-existing workflow from Zookeeper" in {

        val myId= "non-exists"

        val a= Seq.empty[String]

        val result= dao.getWorkflow(myId)

        assertResult("Not Found") {
          result.getOrElse("Not Found")
        }
      }
    }

    "list all workflows" should {
      "create and list a few workflows" in {

        val agents: Seq[Agent]= Seq()

        assert(dao.createWorkflow(WorkFlow.apply("9123", "my workflow 1", "desc", agents)))
        assert(dao.createWorkflow(WorkFlow.apply("9124", "my workflow 2", "desc", agents)))
        assert(dao.createWorkflow(WorkFlow.apply("9125", "my workflow 3", "desc", agents)))

        val workflows: Seq[WorkFlow]= dao.listAll()

        assert(workflows.length == 3)
        assert(workflows(0).description == "desc")
        assert(dao.deleteWorkflow("9123"))
        assert(dao.deleteWorkflow("9124"))
        assert(dao.deleteWorkflow("9125"))
      }
    }

    "list all workflow elements" should {
      "create a few children elements and list them" in {

        val agents: Seq[Agent]= Seq()
        val workflowId= "parent-1234"
        val elementId1= "child-1"
        val elementId2= "child-2"

        assert(dao.createWorkflow(WorkFlow.apply(workflowId, "my workflow 1", "desc", agents)))
        assert(dao.addElementToWorkflow(workflowId, elementId1, "My element value"))
        assert(dao.addElementToWorkflow(workflowId, elementId2, "My element value"))

        val elements= dao.getWorkflowElements(workflowId)
        assert(elements.length == 2)
        assert(elements(0) == "My element value")

        assert(dao.updateWorkflowElement(workflowId, elementId1, "new content"))

        assert(dao.getWorkflowElement(workflowId, elementId1).get == "new content")

        assert(dao.deleteWorkflowElement(workflowId, elementId1))
        assert(dao.deleteWorkflowElement(workflowId, elementId2))

        assert(dao.existsWorkflowElement(workflowId, elementId1) != true)
        assert(dao.existsWorkflowElement(workflowId, elementId2) != true)

        assert(dao.deleteWorkflow(workflowId))
        assert(dao.existsWorkflow(workflowId) != true)
      }
    }

  }


  override def beforeAll() {
    conf= ConfigFactory.load()

    logger.debug("Initializing classes")

    val hosts= conf.getStringList("zookeeper.hosts")
    curator= CuratorFrameworkFactory.newClient(ConfigUtils.getStringFromList(hosts), retryPolicy)
    dao= ZookeeperRepositoryDaoImpl.apply(curator)
    dao.dto.delete(Constants.ZOO_WORKFLOWS_PATH)
  }

  override def afterAll() {
    dao.dto.delete(Constants.ZOO_WORKFLOWS_PATH)
    dao.dto.stop()
  }

}

object ZookeeperIT {
  val logger = Logger.getLogger(getClass().getName())
}
