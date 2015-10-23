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

import com.typesafe.scalalogging.LazyLogging
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api._
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ShouldMatchers, WordSpec}

import scala.pickling.Defaults._
import scala.pickling.binary._

/**
 * Created by aitor on 10/19/15.
 */
@RunWith(classOf[JUnitRunner])
class ZookeeperDTOTest extends WordSpec
  with LazyLogging
  with ShouldMatchers
  with MockitoSugar {

  trait DummyZookeeperDTO {
    val curatorFrameworkMock= mock[CuratorFramework]
  }

  "The ZookeeperDTO" when {

    "call the create method" should {
      "create an element in cluster" in new DummyZookeeperDTO {

        val createBuilder= mock[CreateBuilder]
        val acl= mock[ProtectACLCreateModePathAndBytesable[String]]

        when(createBuilder.creatingParentsIfNeeded()).thenReturn(acl)
        when(curatorFrameworkMock.create()).thenReturn(createBuilder)

        val dto= ZookeeperDTO(curatorFrameworkMock)
        dto.create("/test/my-path", "my value".pickle.value) should be(true)
      }
    }

    "call the update method" should {
      "update an element in cluster" in new DummyZookeeperDTO {

        val builder= mock[SetDataBuilder]
        val acl= mock[ProtectACLCreateModePathAndBytesable[String]]

        val exists= mock[ExistsBuilder]

        when(curatorFrameworkMock.checkExists()).thenReturn(exists)
        when(curatorFrameworkMock.setData()).thenReturn(builder)

        val dto= ZookeeperDTO(curatorFrameworkMock)
        dto.update("/test/my-path", "my value".pickle.value) should be(true)
      }
    }

    "call the exists method" should {
      "check if an element exists in cluster" in new DummyZookeeperDTO {

        val exists= mock[ExistsBuilder]
        when(curatorFrameworkMock.checkExists()).thenReturn(exists)

        val dto= ZookeeperDTO(curatorFrameworkMock)
        dto.exists("/test/my-path") should be(false)
      }
    }

    "call the delete method" should {
      "delete an element of cluster" in new DummyZookeeperDTO {

        val exists= mock[ExistsBuilder]
        val builder= mock[DeleteBuilder]

        when(curatorFrameworkMock.checkExists()).thenReturn(exists)
        when(curatorFrameworkMock.delete()).thenReturn(builder)

        val dto= ZookeeperDTO(curatorFrameworkMock)
        dto.delete("/test/my-path") should be(true)
      }
    }


    "call the start method" should {
      "connect to zookeeper cluster" in new DummyZookeeperDTO {

        when(curatorFrameworkMock.getState()).thenReturn(CuratorFrameworkState.STARTED)

        logger.debug("Connecting to Zookeeper")
        val dto= ZookeeperDTO(curatorFrameworkMock)
        dto.start() should be(true)
      }
    }

    "call the stop method" should {
      "close the connection to zookeeper cluster" in new DummyZookeeperDTO {

        when(curatorFrameworkMock.getState()).thenReturn(CuratorFrameworkState.STOPPED)

        val dto= ZookeeperDTO(curatorFrameworkMock)
        dto.stop() should be(true)
      }
    }



  }
}
