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
package com.stratio.ingestion.interceptor.metadata;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKUtils {

    private static Logger logger = LoggerFactory.getLogger(ZKUtils.class);

    private CuratorFramework client;

    public ZKUtils(String zookeeperCluster) throws Exception {
        logger.debug("Starting Zookeeper...");

        // ZOOKEPER CONNECTION
        client = CuratorFrameworkFactory.newClient(zookeeperCluster, 25 * 1000, 10 * 1000, new ExponentialBackoffRetry(
                1000, 3));
        client.start();
    }

    public boolean existZNode(String path) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        return (stat != null);
    }

    public void removeZNode(String path) throws Exception {
        client.delete().deletingChildrenIfNeeded().forPath(path);
    }

    public void close() throws Exception {
        client.close();
    }

}
