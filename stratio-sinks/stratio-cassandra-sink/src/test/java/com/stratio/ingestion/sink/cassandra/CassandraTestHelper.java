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
package com.stratio.ingestion.sink.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * Helper functions for Cassandra integration testing.
 */
public abstract class CassandraTestHelper {

  private static final Logger log = LoggerFactory.getLogger(CassandraTestHelper.class);

  private CassandraTestHelper() {
  }

  /**
   * Gets an @{link java.net.InetSocketAddress} representing
   * a Cassandra cluster contact point. It takes system properties
   * cassandra.ip and cassandra.port, and defaults to 127.0.0.1:9042.
   *
   * @return @{link java.net.InetSocketAddress} for a Cassandra cluster.
   */
  public static InetSocketAddress getCassandraContactPoint() {

    String cassandraIp = "127.0.0.1";
    try {
      cassandraIp = System.getProperty("cassandra.hosts.0").split(":")[0];
    } catch (NullPointerException ex) {
      log.warn(ex.getLocalizedMessage());
    }
    String cassandraPort = "9042";
    try {
      if(System.getProperty("cassandra.hosts.0").length() > 1) {
        cassandraPort = System.getProperty("cassandra.hosts.0").split(":")[1];
      }
    }  catch (NullPointerException ex) {
      log.warn(ex.getLocalizedMessage());
    }
    try {
      return new InetSocketAddress(InetAddress.getByName(cassandraIp), Integer.parseInt(cassandraPort));
    } catch (UnknownHostException ex) {
      throw new RuntimeException(ex);
    }
  }

}
