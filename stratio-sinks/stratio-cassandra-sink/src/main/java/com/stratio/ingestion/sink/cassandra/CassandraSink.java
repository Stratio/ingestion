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

import static com.stratio.ingestion.sink.cassandra.CassandraUtils.executeCqlScript;
import static com.stratio.ingestion.sink.cassandra.CassandraUtils.getTableMetadata;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.net.HostAndPort;

public class CassandraSink extends AbstractSink implements Configurable {

  private static final Logger log = LoggerFactory.getLogger(CassandraSink.class);

  private static final int DEFAULT_PORT = 9042;
  private static final String DEFAULT_HOST = "localhost:9042";
  private static final int DEFAULT_BATCH_SIZE = 100;
  private static final String DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM.name();
  private static final String DEFAULT_BODY_COLUMN = null;
  private static final boolean DEFAULT_IGNORE_CASE_OPTION = false;

  private static final String CONF_TABLES = "tables";
  private static final String CONF_HOSTS = "hosts";
  private static final String CONF_USERNAME = "username";
  private static final String CONF_PASSWORD = "password";
  private static final String CONF_BATCH_SIZE = "batchSize";
  private static final String CONF_CQL_FILE = "cqlFile";
  private static final String CONF_CONSISTENCY_LEVEL = "consistency";
  private static final String CONF_BODY_COLUMN = "bodyColumn";
  private static final String CONF_IGNORE_CASE = "ignoreCase";
  Cluster cluster;
  Session session;
  List<CassandraTable> tables;
  private SinkCounter sinkCounter;
  private int batchSize;
  private String initCql;
  private List<String> tableStrings;
  private List<InetSocketAddress> contactPoints;
  private String username;
  private String password;
  private String consistency;
  private String bodyColumn;
  private final CounterGroup counterGroup = new CounterGroup();
  private boolean ignoreCase;

  public CassandraSink() {
    super();
  }

  @Override
  public void configure(Context context) {
    contactPoints = new ArrayList<InetSocketAddress>();
    final String hosts = context.getString(CONF_HOSTS, DEFAULT_HOST);
    for (final String host : Splitter.on(',').split(hosts)) {
      try {
        final HostAndPort hostAndPort = HostAndPort.fromString(host).withDefaultPort(DEFAULT_PORT);
        contactPoints.add(new InetSocketAddress(hostAndPort.getHostText(), hostAndPort.getPort()));
      } catch (IllegalArgumentException ex) {
        throw new ConfigurationException("Could not parse host: " + host, ex);
      }
    }

    this.username = context.getString(CONF_USERNAME);
    this.password = context.getString(CONF_PASSWORD);
    this.consistency = context.getString(CONF_CONSISTENCY_LEVEL, DEFAULT_CONSISTENCY_LEVEL);
    this.bodyColumn = context.getString(CONF_BODY_COLUMN, DEFAULT_BODY_COLUMN);
    this.ignoreCase = context.getBoolean(CONF_IGNORE_CASE, DEFAULT_IGNORE_CASE_OPTION);

    final String tablesString = StringUtils.trimToNull(context.getString(CONF_TABLES));
    if (tablesString == null) {
      throw new ConfigurationException(String.format("%s is mandatory", CONF_TABLES));
    }
    this.tableStrings = Arrays.asList(tablesString.split(","));

    final String cqlFile = StringUtils.trimToNull(context.getString(CONF_CQL_FILE));
    if (cqlFile != null) {
      try {
        this.initCql = IOUtils.toString(new FileInputStream(cqlFile));
      } catch (IOException ex) {
        throw new ConfigurationException("Cannot read CQL file: " + cqlFile, ex);
      }
    }

    this.batchSize = context.getInteger(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE);
    this.sinkCounter = new SinkCounter(this.getName());
  }

  @Override
  public synchronized void start() {

    // Connect to Cassandra cluster
    Cluster.Builder clusterBuilder = Cluster.builder()
        .addContactPointsWithPorts(contactPoints);
    if (!Strings.isNullOrEmpty(username) && !Strings.isNullOrEmpty(password)) {
      clusterBuilder = clusterBuilder.withCredentials(username, password);
    }
    this.cluster = clusterBuilder.build();
    this.session = this.cluster.connect();

    // Initialize database if CQL script is provided
    executeCqlScript(session, initCql);

    tables = new ArrayList<CassandraTable>();
    for (final String tableString : tableStrings) {
      final String[] fields = tableString.split("\\.");
      if (fields.length != 2) {
        throw new IllegalArgumentException("Invalid format: " + tableString);
      }
      final String keyspace = fields[0];
      final String table = fields[1];
      final TableMetadata tableMetadata = getTableMetadata(session, keyspace, table);
      tables.add(new CassandraTable(session, tableMetadata, ConsistencyLevel.valueOf(consistency), bodyColumn, ignoreCase));
    }

    this.sinkCounter.start();
    super.start();
  }

  @Override
  public synchronized void stop() {
    if (session != null && !session.isClosed()) {
      try {
        session.close();
      } catch (RuntimeException ex) {
        log.error("Error while closing session", ex);
      }
    }
    if (cluster != null && !cluster.isClosed()) {
      try {
        cluster.close();
      } catch (RuntimeException ex) {
        log.error("Error while closing cluster", ex);
      }
    }
    this.sinkCounter.stop();
    super.stop();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Status process() throws EventDeliveryException {
    log.debug("Executing CassandraSink.process()...");
    Status status = Status.READY;
    Channel channel = getChannel();
    Transaction txn = channel.getTransaction();
    try {
      txn.begin();
      int count;
      List<Event> eventList= new ArrayList<Event>();
      for (count = 0; count < batchSize; ++count) {
        Event event = channel.take();

        if (event == null) {
          break;
        }
        eventList.add(event);
      }

      if (count <= 0) {
        sinkCounter.incrementBatchEmptyCount();
        counterGroup.incrementAndGet("channel.underflow");
        status = Status.BACKOFF;
      } else {
        if (count < batchSize) {
          sinkCounter.incrementBatchUnderflowCount();
          status = Status.BACKOFF;
        } else {
          sinkCounter.incrementBatchCompleteCount();
        }

        for (final CassandraTable table : tables) {
          table.save(eventList);
        }

        sinkCounter.addToEventDrainAttemptCount(count);
        //client.execute();
      }
      txn.commit();
      sinkCounter.addToEventDrainSuccessCount(count);
      counterGroup.incrementAndGet("transaction.success");
    } catch (Throwable ex) {
      try {
        txn.rollback();
        counterGroup.incrementAndGet("transaction.rollback");
      } catch (Exception ex2) {
        log.error(
                "Exception in rollback. Rollback might not have been successful.",
                ex2);
      }

      if (ex instanceof Error || ex instanceof RuntimeException) {
        log.error("Failed to commit transaction. Transaction rolled back.",
                ex);
        Throwables.propagate(ex);
      } else {
        log.error("Failed to commit transaction. Transaction rolled back.",
                ex);
        throw new EventDeliveryException(
                "Failed to commit transaction. Transaction rolled back.", ex);
      }
    } finally {
      txn.close();
    }
    return status;

  }


}
