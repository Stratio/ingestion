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

import com.stratio.ingestion.api.core.dao.ZookeeperDTO
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ShouldMatchers, WordSpec}


/**
 * Created by aitor on 10/19/15.
 */
@RunWith(classOf[JUnitRunner])
class ZookeeperDTOTest extends WordSpec
  with ShouldMatchers
  with MockitoSugar {

  trait DummyZookeeperDTO {
    val curatorFrameworkMock= mock[CuratorFramework]
  }

  "The ZookeeperDTO" when {
    "call the start method" should {
      "connect to zookeeper cluster" in new DummyZookeeperDTO {
        info("Checking if Zookeeper cluster is up")

        when(curatorFrameworkMock.getState()).thenReturn(CuratorFrameworkState.STARTED)

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
