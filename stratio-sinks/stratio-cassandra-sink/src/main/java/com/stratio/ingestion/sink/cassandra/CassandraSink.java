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
import com.datastax.driver.core.exceptions.DriverException;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.net.HostAndPort;

/**
 * The Cassandra Sink component allows to save Flume-flow events into Cassandra.
 *It tries to find the defined fields in the event's headers. If a "data" field is defined, it will take the body's event instead of a header.
 *
 * Available configuration parameters are:
 *
 * <p><ul>
 * <li><tt>tables</tt> <em>(string, required)</em>: One or more table names separated with commas.
 * Table names must be fully qualified with keyspace (e.g. keyspace1.table1,keyspace2.table2)</li>
 * <li><tt>hosts</tt> <em>(string, required)</em>: A comma-separated list of Cassandra hosts. It is recommended to specify
 * at least two host of the cluster. The result of the cluster will be auto-discovered. (Default: localhost:9042)</li>
 * <li><tt>username</tt> <em>(string)</em>: A valid database username.</li>
 * <li><tt>password</tt> <em>(string)</em>: Password.</li>
 * <li><tt>batchSize</tt> <em>(integer)</em>: The size to batch insert statement. We recommend 100 as an optimum value
 * to this property.
 * Please do not forget increase the channel.capacity property on your channel component over the sink.batchSize property. (Default: 100)</li>
 * <li><tt>consistency</tt> <em>(string)</em>: The consistency level for this insert. Default value are QUORUM, available
 * options are described here: [Cassandra data consistency](http://www.datastax.com/documentation/cassandra/2.0/cassandra/dml/dml_config_consistency_c.html)
 * (Default: QUORUM)</em></li>
 * <li><tt>cqlFile</tt> <em>(string)</em>: Path to a CQL file with initialization statements such as keyspace and table creation.</li>
 * </ul></p>
 *
 */

public class CassandraSink extends AbstractSink implements Configurable {

  private static final Logger log = LoggerFactory.getLogger(CassandraSink.class);

  private static final int DEFAULT_PORT = 9042;
  private static final String DEFAULT_HOST = "localhost:9042";
  private static final int DEFAULT_BATCH_SIZE = 100;
  private static final String DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM.name();
  private static final String DEFAULT_BODY_COLUMN = null;

  private static final String CONF_TABLES = "tables";
  private static final String CONF_HOSTS = "hosts";
  private static final String CONF_USERNAME = "username";
  private static final String CONF_PASSWORD = "password";
  private static final String CONF_BATCH_SIZE = "batchSize";
  private static final String CONF_CQL_FILE = "cqlFile";
  private static final String CONF_CONSISTENCY_LEVEL = "consistency";
  private static final String CONF_BODY_COLUMN = "bodyColumn";
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
      tables.add(new CassandraTable(session, tableMetadata, ConsistencyLevel.valueOf(consistency), bodyColumn));
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

  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.BACKOFF;
    Transaction txn = this.getChannel().getTransaction();
    try {
      txn.begin();
      List<Event> eventList = this.takeEventsFromChannel(
          this.getChannel(), this.batchSize);
      status = Status.READY;
      if (!eventList.isEmpty()) {
        if (eventList.size() == this.batchSize) {
          this.sinkCounter.incrementBatchCompleteCount();
        } else {
          this.sinkCounter.incrementBatchUnderflowCount();
        }
        for (final CassandraTable table : tables) {
          table.save(eventList);
        }
        this.sinkCounter.addToEventDrainSuccessCount(eventList.size());
      } else {
        this.sinkCounter.incrementBatchEmptyCount();
      }
      txn.commit();
      status = Status.READY;
    } catch (Throwable t) {
      try {
        txn.rollback();
      } catch (Exception e) {
        log.error("Exception in rollback. Rollback might not have been successful.", e);
      }
      log.error("Failed to commit transaction. Rolled back.", t);
      if (t instanceof DriverException || t instanceof IllegalArgumentException) {
        throw new EventDeliveryException("Failed to commit transaction. Rolled back.", t);
      } else { // (t instanceof Error || t instanceof RuntimeException)
        Throwables.propagate(t);
      }
    } finally {
      txn.close();
    }
    return status;
  }

  private List<Event> takeEventsFromChannel(final Channel channel, final int eventsToTake) {
    final List<Event> events = new ArrayList<Event>();
    for (int i = 0; i < eventsToTake; i++) {
      this.sinkCounter.incrementEventDrainAttemptCount();
      final Event event = channel.take();
      if (event != null) {
        this.sinkCounter.incrementEventDrainSuccessCount();
        events.add(event);
      }
    }
    return events;
  }

}
