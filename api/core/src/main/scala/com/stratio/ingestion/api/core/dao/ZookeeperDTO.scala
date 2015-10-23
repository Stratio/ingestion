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
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry

import scala.util.Try

/**
 * Created by aitor on 10/16/15.
 */
case class ZookeeperDTO(template: CuratorFramework) extends LazyLogging {

  private val curatorZookeeperClient= template

  def create(path: String, contents: Array[Byte]): Boolean = {
    curatorZookeeperClient.create().creatingParentsIfNeeded().forPath(path, contents)
    true
  }

  def update(path: String, contents: Array[Byte]): Boolean = {
    curatorZookeeperClient.setData().forPath(path, contents)
    true
  }

  def exists(path: String): Boolean = {
    val stat= curatorZookeeperClient.checkExists().forPath(path)
    stat != null
  }

  def delete(path: String): Boolean = {
    val stat= curatorZookeeperClient.checkExists().forPath(path)
    if (stat != null)
      curatorZookeeperClient.delete().forPath(path)
    true
  }

  def getElementData(path: String): Option[Array[Byte]] = {
    Try {
      curatorZookeeperClient.getData().forPath(path)
    }.toOption
  }

  def getChildren(path: String): Seq[Array[Byte]] = {
    var children: Seq[Array[Byte]]= Seq()
    val stat= curatorZookeeperClient.checkExists().forPath(path)
    if (stat != null) {
      val list= curatorZookeeperClient.getChildren.forPath(path).toArray

      list.foreach {
        id => logger.debug("Element found: " + id)
          val childStat= curatorZookeeperClient.checkExists().forPath(path + "/" + id)
          if (childStat != null) {
            children :+= getElementData(path + "/" + id).get
            logger.debug("Adding children: " + id)
          }
      }
    }
    children
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
    curatorZookeeperClient.getState == CuratorFrameworkState.STARTED
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

  def initialize(template: CuratorFramework, retryPolicy: RetryPolicy=
    new ExponentialBackoffRetry(DEFAULT_SLEEP_TIME, DEFAULT_MAX_RETRIES)): ZookeeperDTO = {

    val dto= new ZookeeperDTO(template)
    if (!dto.isStarted())
      dto.start()
    dto
  }

  def getInstance(connectionUrl: String, retryPolicy: RetryPolicy=
    new ExponentialBackoffRetry(DEFAULT_SLEEP_TIME, DEFAULT_MAX_RETRIES)):ZookeeperDTO = {

    apply(CuratorFrameworkFactory.newClient(connectionUrl,retryPolicy))
  }



}