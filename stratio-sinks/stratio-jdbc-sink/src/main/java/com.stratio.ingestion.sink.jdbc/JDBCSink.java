package com.stratio.ingestion.sink.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCSink extends AbstractSink implements Configurable {

    private static final Logger log = LoggerFactory.getLogger(JDBCSink.class);

    private static final int DEFAULT_BATCH_SIZE = 20;
    private static final String CONF_DRIVER = "driver";
    private static final String CONF_CONNECTION_STRING = "connectionString";
    private static final String CONF_SQL_DIALECT = "sqlDialect";
    private static final String CONF_TABLE = "table";
    private static final String CONF_BATCH_SIZE = "batchSize";
    private static final String CONF_SQL = "sql";

    private Connection connection;
    private DSLContext create;
    private SinkCounter sinkCounter;
    private int batchsize;
    private QueryGenerator queryGenerator;

    public JDBCSink() {
        super();
    }

    @Override
    public void configure(Context context) {
        final String driver = context.getString(CONF_DRIVER);
        final String connectionString = context.getString(CONF_CONNECTION_STRING);

        this.batchsize = context.getInteger(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE);

        try {
            Class.forName(driver).newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            throw new JDBCSinkException(ex);
        }
        connection = null;
        try {
            connection = DriverManager.getConnection(connectionString);
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
        Status status = Status.BACKOFF;
        Transaction transaction = this.getChannel().getTransaction();
        try {
            transaction.begin();
            List<Event> eventList = this.takeEventsFromChannel(
                    this.getChannel(), this.batchsize);
            status = Status.READY;
            if (!eventList.isEmpty()) {
                if (eventList.size() == this.batchsize) {
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
        List<Event> events = new ArrayList<>();
        for (int i = 0; i < eventsToTake; i++) {
            this.sinkCounter.incrementEventDrainAttemptCount();
            events.add(channel.take());
        }
        events.removeAll(Collections.singleton(null));
        return events;
    }

}