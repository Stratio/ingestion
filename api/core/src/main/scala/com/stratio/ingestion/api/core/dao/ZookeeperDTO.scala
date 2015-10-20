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

import com.stratio.ingestion.api.core.utils.LoggerComponent
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry

/**
 * Created by aitor on 10/16/15.
 */
case class ZookeeperDTO(template: CuratorFramework) extends LoggerComponent {

  private val curatorZookeeperClient= template

  def create(path: String, contents: Array[Byte]): Boolean = {
    //curatorZookeeperClient.create().forPath(path, contents)
    true
  }

  def start(): Boolean = {
    curatorZookeeperClient.start()
    isStarted()
  }

  def stop(): Boolean = {
    if (curatorZookeeperClient.getState != CuratorFrameworkState.STOPPED)
      curatorZookeeperClient.close()
    true
  }

  def isStarted(): Boolean = {
    if (curatorZookeeperClient.getState() == CuratorFrameworkState.STARTED)
      true
    else
      false
  }


  def getState(): CuratorFrameworkState = {
    curatorZookeeperClient.getState
  }

}

object ZookeeperDTO {

  // Initial amount of time to wait between retries
  private val DEFAULT_SLEEP_TIME= 1000

  // Max number of times to retry
  private val DEFAULT_MAX_RETRIES= 3

  def apply(template: CuratorFramework, retryPolicy: RetryPolicy=
  new ExponentialBackoffRetry(DEFAULT_SLEEP_TIME, DEFAULT_MAX_RETRIES)): ZookeeperDTO = {

    val dto= ZookeeperDTO(template, retryPolicy)
    if (!dto.isStarted())
      dto.start()
    dto
  }

  def getInstance(connectionUrl: String, retryPolicy: RetryPolicy=
    new ExponentialBackoffRetry(DEFAULT_SLEEP_TIME, DEFAULT_MAX_RETRIES)):ZookeeperDTO = {

    apply(CuratorFrameworkFactory.newClient(connectionUrl,retryPolicy))
  }



}