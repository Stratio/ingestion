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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * Helper functions for Cassandra integration testing.
 */
public abstract class CassandraTestHelper {

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
    String cassandraIp = System.getProperty("cassandra.ip");
    if (cassandraIp == null) {
      cassandraIp = "127.0.0.1";
    }
    String cassandraPort = System.getProperty("cassandra.port");
    if (cassandraPort == null) {
      cassandraPort = "9042";
    }
    try {
      return new InetSocketAddress(InetAddress.getByName(cassandraIp), Integer.parseInt(cassandraPort));
    } catch (UnknownHostException ex) {
      throw new RuntimeException(ex);
    }
  }

}
