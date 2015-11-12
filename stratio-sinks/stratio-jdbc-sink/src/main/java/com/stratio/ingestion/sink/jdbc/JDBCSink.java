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
package com.stratio.ingestion.sink.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/**
 * Saves Flume events to any database with a JDBC driver. It can operate either
 * with automatic headers-to-tables mapping or with custom SQL queries.
 *
 * Available configuration parameters are:
 *
 * <p><ul>
 * <li><tt>driver</tt> <em>(string, required)</em>: The driver class (e.g.
 *      <tt>org.h2.Driver</tt>, <tt>org.postgresql.Driver</tt>). <strong>NOTE:</strong>
 *      Stratio JDBC Sink does not include any JDBC driver. You must add a JDBC
 *      driver to your Flume classpath.</li>
 * <li><tt>connectionString</tt> <em>(string, required)</em>: A valid
 *      connection string to a database. Check the documentation for your JDBC driver
 *      for more information.</li>
 * <li><tt>username</tt> <em>(string)</em>: A valid database username.</li>
 * <li><tt>password</tt> <em>(string)</em>: Password.</li>
 * <li><tt>sqlDialect</tt> <em>(string, required)</em>: The SQL dialect of your
 *      database. This should be one of the following: <tt>CUBRID</tt>, <tt>DERBY</tt>,
 *      <tt>FIREBIRD</tt>, <tt>H2</tt>, <tt>HSQLDB</tt>, <tt>MARIADB</tt>, <tt>MYSQL</tt>,
 *      <tt>POSTGRES</tt>, <tt>SQLITE</tt>.</li>
 * <li><tt>table</tt> <em>(string)</em>: A table to store your events.
 *      <em>This is only used for automatic mapping.</em></li>
 * <li><tt>sql</tt> <em>(string)</em>: A custom SQL query to use. If specified,
 *      this query will be used instead of automatic mapping. E.g.
 *      <tt>INSERT INTO tweets (text, num_hashtags, timestamp) VALUES (${body:string}, ${header.numberOfHashtags:integer}, ${header.date:timestamp})</tt>.
 *      Note the variable format: the first part is either <tt>body</tt> or
 *      <tt>header.yourHeaderName</tt> and then the SQL type.</li>
 * <li><tt>batchSize</tt> <em>(integer)</em>: Number of events that will be grouped
 *      in the same query and transaction. Defaults to <tt>20</tt>.</li>
 * </ul></p>
 *
 */
public class JDBCSink extends AbstractSink implements Configurable {

    private static final Logger log = LoggerFactory.getLogger(JDBCSink.class);

    private static final int DEFAULT_BATCH_SIZE = 20;
    private static final String CONF_DRIVER = "driver";
    private static final String CONF_CONNECTION_STRING = "connectionString";
    private static final String CONF_SQL_DIALECT = "sqlDialect";
    private static final String CONF_TABLE = "table";
    private static final String CONF_BATCH_SIZE = "batchSize";
    private static final String CONF_SQL = "sql";
    private static final String CONF_USER = "username";
    private static final String CONF_PASSWORD = "password";

    private Connection connection;
    private DSLContext create;
    private SinkCounter sinkCounter;
    private int batchSize;
    private QueryGenerator queryGenerator;
    private final CounterGroup counterGroup = new CounterGroup();

    public JDBCSink() {
        super();
    }

    @Override
    public void configure(Context context) {
        final String driver = context.getString(CONF_DRIVER);
        final String connectionString = context.getString(CONF_CONNECTION_STRING);

        this.batchSize = context.getInteger(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE);

        try {
            Class.forName(driver).newInstance();
        } catch (ClassNotFoundException ex) {
            throw new JDBCSinkException(ex);
        } catch (InstantiationException ex) {
          throw new JDBCSinkException(ex);
        } catch (IllegalAccessException ex) {
          throw new JDBCSinkException(ex);
        }
        String username = context.getString(CONF_USER);
        String password = context.getString(CONF_PASSWORD);

        connection = null;
        try {
            if(Strings.isNullOrEmpty(username) || Strings.isNullOrEmpty(password)){ //Non authorized
                connection = DriverManager.getConnection(connectionString);
            } else { //Authorized
                //TODO: check all possible connections to remove this if
                if (CONF_SQL_DIALECT.equals("H2")) {
                    connection = DriverManager.getConnection(connectionString+";USER="+username+";PASSWORD="+password);
                } else {
                    connection = DriverManager.getConnection(connectionString, username, password);
                }
            }

        } catch (SQLException ex) {
            throw new JDBCSinkException(ex);
        }
        try {
            connection.setAutoCommit(false);
        } catch (SQLException ex) {
            throw new JDBCSinkException(ex);
        }

        final ConnectionProvider connectionProvider = new DefaultConnectionProvider(connection);
        final SQLDialect sqlDialect = SQLDialect.valueOf(context.getString(CONF_SQL_DIALECT).toUpperCase(Locale.ENGLISH));

        create = DSL.using(connectionProvider, sqlDialect);

        final String sql = context.getString(CONF_SQL);
        if (sql == null) {
            this.queryGenerator = new MappingQueryGenerator(create, context.getString(CONF_TABLE));
        } else {
            this.queryGenerator = new TemplateQueryGenerator(sqlDialect, sql);
        }

        this.sinkCounter = new SinkCounter(this.getName());
    }

    @Override
    public Status process() throws EventDeliveryException {

        log.debug("Executing JDBCSink.process()...");
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

                final boolean success = this.queryGenerator.executeQuery(create, eventList);
                if (!success) {
                    throw new JDBCSinkException("Query failed");
                }
                connection.commit();

                sinkCounter.addToEventDrainAttemptCount(count);
            }
            txn.commit();
            sinkCounter.addToEventDrainSuccessCount(count);
            counterGroup.incrementAndGet("transaction.success");
        } catch (Throwable t) {
            log.error("Exception during process", t);
            try {
                connection.rollback();
            } catch (SQLException ex) {
                log.error("Exception on rollback", ex);
            } finally {
                txn.rollback();
                status = Status.BACKOFF;
                this.sinkCounter.incrementConnectionFailedCount();
                if (t instanceof Error) {
                    throw new JDBCSinkException(t);
                }
            }
        } finally {
            txn.close();
        }
        return status;
    }


    public Status BACKUP_process() throws EventDeliveryException {
        Status status = Status.BACKOFF;
        Transaction transaction = this.getChannel().getTransaction();
        try {
            transaction.begin();
            List<Event> eventList = this.takeEventsFromChannel(
                    this.getChannel(), this.batchSize);
            status = Status.READY;
            if (!eventList.isEmpty()) {
                if (eventList.size() == this.batchSize) {
                    this.sinkCounter.incrementBatchCompleteCount();
                } else {
                    this.sinkCounter.incrementBatchUnderflowCount();
                }

                final boolean success = this.queryGenerator.executeQuery(create, eventList);
                if (!success) {
                    throw new JDBCSinkException("Query failed");
                }
                connection.commit();

                this.sinkCounter.addToEventDrainSuccessCount(eventList.size());
            } else {
                this.sinkCounter.incrementBatchEmptyCount();
            }

            transaction.commit();
            status = Status.READY;
        } catch (Throwable t) {
            log.error("Exception during process", t);
            try {
                connection.rollback();
            } catch (SQLException ex) {
                log.error("Exception on rollback", ex);
            } finally {
                transaction.rollback();
                status = Status.BACKOFF;
                this.sinkCounter.incrementConnectionFailedCount();
                if (t instanceof Error) {
                    throw new JDBCSinkException(t);
                }
            }
        } finally {
            transaction.close();
        }
        return status;
    }

    @Override
    public synchronized void start() {
        this.sinkCounter.start();
        super.start();
    }

    @Override
    public synchronized void stop() {
        this.sinkCounter.stop();
        super.stop();
    }

    private List<Event> takeEventsFromChannel(Channel channel, int eventsToTake) {
        List<Event> events = new ArrayList<Event>();
        for (int i = 0; i < eventsToTake; i++) {
            this.sinkCounter.incrementEventDrainAttemptCount();
            events.add(channel.take());
        }
        events.removeAll(Collections.singleton(null));
        return events;
    }

}
