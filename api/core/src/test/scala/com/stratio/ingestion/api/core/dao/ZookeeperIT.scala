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
package scala.com.stratio.ingestion.api.core.dao

import com.stratio.ingestion.api.core.dao.ZookeeperRepositoryDaoImpl
import com.stratio.ingestion.api.core.utils._
import com.stratio.ingestion.api.model.commons.{WorkFlow, Agent}
import com.typesafe.config.{Config, ConfigFactory}
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
  with ShouldMatchers
  with MockitoSugar
  with BeforeAndAfterAll {

  final def logger = ZookeeperIT.logger

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
        assert(dao.getWorkflow(myId).name == "my workflow")
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
          result.name
        }
        //assertEquals("my workflow", dao.getWorkflow("1234").name)
      }
    }

  }


  override def beforeAll() {
    conf= ConfigFactory.load()

    logger.debug("Initializing classes")

    val hosts= conf.getStringList("zookeeper.hosts")
    curator= CuratorFrameworkFactory.newClient(ConfigUtils.getStringFromList(hosts), retryPolicy)
    dao= ZookeeperRepositoryDaoImpl.apply(curator)

  }

  override def afterAll() {

  }

}

object ZookeeperIT {
  val logger = Logger.getLogger(getClass().getName())
}
