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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.DriverException;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;

public class CassandraSink extends AbstractSink implements Configurable {

    private static final Logger log = LoggerFactory.getLogger(CassandraSink.class);

    private static final int DEFAULT_PORT = 9042;
    private static final String DEFAULT_CLUSTER = "Test Cluster";
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final String DEFAULT_CONSISTENCY_LEVEL = "QUORUM";
    private static final String DEFAULT_BODY_COLUMN = null;

	private static final String DEFAULT_ITEM_SEPARATOR = ",";
	private static final String DEFAULT_MAP_VALUE_SEPARATOR = ":";
	private static final String DEFAULT_MAP_KEY_TYPE = "TEXT";
	private static final String DEFAULT_MAP_VALUE_TYPE = "INT";
	private static final String DEFAULT_LIST_VALUE_TYPE = "TEXT";
	
    private static final String CONF_TABLE = "table";
    private static final String CONF_PORT = "port";
    private static final String CONF_CLUSTER = "cluster";
    private static final String CONF_HOST = "host";
    private static final String CONF_USERNAME = "username";
    private static final String CONF_PASSWORD = "password";
    private static final String CONF_BATCH_SIZE = "batchSize";
    private static final String CONF_CQL_FILE = "cqlFile";
    private static final String CONF_CONSISTENCY_LEVEL = "consistency";
    private static final String CONF_BODY_COLUMN = "bodyColumn";

    private static final String CONF_ITEM_SEPARATOR = "itemSeparator";
    private static final String CONF_MAP_VALUE_SEPARATOR = "mapValueSeparator";
    private static final String CONF_MAP_KEY_TYPE = "mapKeyType";
    private static final String CONF_MAP_VALUE_TYPE = "mapValueType";
	  private static final String CONF_LIST_VALUE_TYPE = "listValueType";

    private SinkCounter sinkCounter;
    private Cluster cluster;
    private Session session;
    private int batchsize;

    private String initCql;
    private List<String> tableStrings;
    private List<CassandraTable> tables;
    private String host;
    private int port;
    private String username;
    private String password;
    private String clusterName;
    private String consistency;
    private String bodyColumn;
    private String dateFormat;
    private String itemSeparator;
    private String mapValueSeparator;
    private String mapKeyType;
    private String mapValueType;
    private String listValueType;

    public CassandraSink() {
        super();
    }

    @Override
    public void configure(Context context) {
        this.host = context.getString(CONF_HOST, DEFAULT_HOST);
        this.port = context.getInteger(CONF_PORT, DEFAULT_PORT);
        this.username = context.getString(CONF_USERNAME);
        this.password = context.getString(CONF_PASSWORD);
        this.clusterName = context.getString(CONF_CLUSTER, DEFAULT_CLUSTER);
        this.consistency = context.getString(CONF_CONSISTENCY_LEVEL, DEFAULT_CONSISTENCY_LEVEL);
        this.bodyColumn = context.getString(CONF_BODY_COLUMN, DEFAULT_BODY_COLUMN);

        final String tablesString = StringUtils.trimToNull(context.getString(CONF_TABLE));
        if (tablesString == null) {
            throw new ConfigurationException(String.format("%s is mandatory", CONF_TABLE));
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

        this.batchsize = context.getInteger(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE);
        this.sinkCounter = new SinkCounter(this.getName());

        this.itemSeparator = context.getString(CONF_ITEM_SEPARATOR, DEFAULT_ITEM_SEPARATOR);
        this.mapValueSeparator = context.getString(CONF_MAP_VALUE_SEPARATOR, DEFAULT_MAP_VALUE_SEPARATOR);
        this.mapKeyType = context.getString(CONF_MAP_KEY_TYPE, DEFAULT_MAP_KEY_TYPE);
        this.mapValueType = context.getString(CONF_MAP_VALUE_TYPE, DEFAULT_MAP_VALUE_TYPE);
        this.listValueType = context.getString(CONF_LIST_VALUE_TYPE, DEFAULT_LIST_VALUE_TYPE);
    }

    @Override
    public synchronized void start() {

        // Connect to Cassandra cluster
        Cluster.Builder clusterBuilder = Cluster.builder()
            .addContactPoint(host)
            .withPort(port);
        if (!Strings.isNullOrEmpty(username) && !Strings.isNullOrEmpty(password)) {
            clusterBuilder = clusterBuilder.withCredentials(username, password);
        }
        this.cluster = clusterBuilder.build();
        this.session = this.cluster.connect();

        // Initialize database if CQL script is provided
        if (initCql != null) {
            final List<String> lines = new ArrayList<String>();
            for (final String line : Splitter.on("\n").split(initCql)) {
                lines.add(line.trim());
            }
            for (final String cql : Joiner.on(" ").join(lines).split(";")) {
                this.session.execute(cql);
            }
        }

        tables = new ArrayList<CassandraTable>();
        for (final String tableString : tableStrings) {
            final String[] fields = tableString.split("\\.");
            if (fields.length != 2) {
                throw new IllegalArgumentException("Invalid format: " + tableString);
            }
            final String keyspace = fields[0];
            final String table = fields[1];
            final EventParser parser = new EventParser(getColumnDefinition(getMetadata(session, keyspace, table)), bodyColumn);
            tables.add(new CassandraTable(session, keyspace, table, parser, ConsistencyLevel.valueOf(consistency)));
        }

        this.sinkCounter.start();
        super.start();
    }

    @Override
    public synchronized void stop() {
        this.session.close();
        this.cluster.close();
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
                this.getChannel(), this.batchsize);
            status = Status.READY;
            if (!eventList.isEmpty()) {
                if (eventList.size() == this.batchsize) {
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

    private ColumnDefinition getColumnDefinition(TableMetadata tableMetadata) {
        List<FieldDefinition> fields = new ArrayList<FieldDefinition>();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            FieldDefinition field = new FieldDefinition();
            field.setColumnName(column.getName());
            field.setType(column.getType().getName().name().toUpperCase());
            field.setItemSeparator(itemSeparator);
            field.setListValueType(listValueType);
            field.setMapKeyType(mapKeyType);
            field.setMapValueSeparator(mapValueSeparator);
            field.setMapValueType(mapValueType);
            fields.add(field);
        }
        ColumnDefinition definition = new ColumnDefinition();
        definition.setFields(fields);
        return definition;
    }

    private static TableMetadata getMetadata(final Session session, final String keyspace, final String table) {
        final KeyspaceMetadata keyspaceMetadata = session.getCluster().getMetadata().getKeyspace(keyspace);
        if (keyspaceMetadata == null) {
            throw new IllegalStateException(String.format("Keyspace %s does not exist", keyspace));
        }
        final TableMetadata tableMetadata = keyspaceMetadata.getTable(table);
        if (tableMetadata == null) {
            throw new IllegalStateException(String.format("Table %s.%s does not exist", keyspace, table));
        }
        return tableMetadata;
    }

    private List<Event> takeEventsFromChannel(Channel channel, int eventsToTake) {
        List<Event> events = new ArrayList<Event>();
        for (int i = 0; i < eventsToTake; i++) {
            this.sinkCounter.incrementEventDrainAttemptCount();
            events.add(channel.take());
        }
        events.removeAll(Collections.singleton((Event)null));
        return events;
    }

}
