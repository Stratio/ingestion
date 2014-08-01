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
package com.stratio.ingestion.sink.kafka;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import com.google.common.io.Files;

public class ZookeeperServer {
    private static final int WAIT_SECONDS = 5;
    public static final int CLIENT_PORT = 2281;
    private static final int NUM_CONNECTIONS = 5000;
    private static final int TICK_TIME = 2000;

    private NIOServerCnxnFactory standaloneServerFactory;

    /**
     * Set embedded zookeeper up and spawn it in a new thread.
     * 
     * @throws TTransportException
     * @throws IOException
     * @throws InterruptedException
     */
    public void start() throws Exception {

        File dir = Files.createTempDir();
        String dirPath = dir.getAbsolutePath();
        System.out.println("Storing ZooKeeper files in " + dirPath);

        ZooKeeperServer server = new ZooKeeperServer(dir, dir, TICK_TIME);
        standaloneServerFactory = new NIOServerCnxnFactory();
        standaloneServerFactory.configure(new InetSocketAddress(CLIENT_PORT), NUM_CONNECTIONS);

        standaloneServerFactory.startup(server); // start the server.

        TimeUnit.SECONDS.sleep(WAIT_SECONDS);
    }

    public void shutdown() throws IOException {
        standaloneServerFactory.shutdown();
    }
}
