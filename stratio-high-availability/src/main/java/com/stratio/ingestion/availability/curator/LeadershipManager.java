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
package com.stratio.ingestion.availability.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.EOFException;
import java.io.IOException;

/**
 * Created by epeinado on 26/05/15.
 */
public class LeadershipManager {

    private CuratorFramework client;
    private String latchpath;
    private String id;
    private LeaderLatch leaderLatch;

    public LeadershipManager(String connString, String latchpath, String id) {
        this.client = CuratorFrameworkFactory.newClient(connString, 5000, 15000, new ExponentialBackoffRetry(1000, Integer.MAX_VALUE));
        this.latchpath = latchpath;
        this.id = id;
    }

    public void start() throws Exception {
        client.start();
        client.getZookeeperClient().blockUntilConnectedOrTimedOut();
        leaderLatch = new LeaderLatch(client, latchpath, id);
        leaderLatch.start();
    }

    public boolean isLeader() {
        return leaderLatch.hasLeadership();
    }

    public Participant currentLeader() throws Exception {
        return leaderLatch.getLeader();
    }

    public void close() throws IOException {
        leaderLatch.close();
        client.close();
    }

    public void waitForLeadership() throws InterruptedException, EOFException {
        leaderLatch.await();
    }

    public static void main (String[] args) throws Exception {
        String latchPath = "/latch";
        String connStr = "127.0.0.1:2181";
        LeadershipManager node1 = new LeadershipManager(connStr, latchPath, "node-1");
        LeadershipManager node2 = new LeadershipManager(connStr, latchPath, "node-2");
        node1.start();
        node2.start();

        for (int i = 0; i < 10; i++) {
            System.out.println("node 1 thinks the leader is " + node1.currentLeader());
            System.out.println("node 2 thinks the leader is " + node2.currentLeader());
            Thread.sleep(10000);
        }

        node1.close();

        System.out.println("node 2 thinks the leader is " + node2.currentLeader());

        node2.close();

    }

}
