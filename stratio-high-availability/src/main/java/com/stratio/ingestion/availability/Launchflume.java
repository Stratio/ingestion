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
package com.stratio.ingestion.availability;

import com.stratio.ingestion.availability.curator.LeadershipManager;
import org.apache.flume.node.Application;
import org.kohsuke.randname.RandomNameGenerator;

import java.util.Random;

public class Launchflume {

    private static final String PATH = "/latch";
    private static final String connectionString = "127.0.0.1:2181";

    public static void main(String[] args) throws Exception {

        Random r = new Random();
        RandomNameGenerator rnd = new RandomNameGenerator(r.nextInt());

        LeadershipManager node = new LeadershipManager(connectionString, PATH, rnd.next());
        try {
            node.start();
            waitForLeadership(node, args);

        } finally {
            node.close();
        }

    }

    private static void waitForLeadership(LeadershipManager node, String[] args) throws Exception {
        while (!node.isLeader()) {
            System.out.println("Waiting or leadership...");
            node.waitForLeadership();
        }
        runLeadership(node, args);
    }

    private static void runLeadership(LeadershipManager node, String[] args) throws Exception {
        System.out.println("I'm the leader " + node.currentLeader());
        Application.main(args);


        while (node.isLeader()) {
            Thread.sleep(5000);
        }

        waitForLeadership(node, args);

    }


}
