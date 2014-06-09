package com.stratio.ingestion.sink.cassandra;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;

public class CassandraSink extends AbstractSink implements Configurable {

	private static final String CASSANDRA_DEFAULT_CONSISTENCY = "QUORUM";
	private static final int CASSANDRA_DEFAULT_BATCH_SIZE = 20;
    private static final String CONF_TABLE = "table";
    private static final String CONF_PRIMARY_KEY = "primaryKey";
    private static final String CONF_KEYSPACE = "keyspace";
    private static final String CONF_PORT = "port";
    private static final String CONF_CLUSTER = "cluster";
    private static final String CONF_HOST = "host";
    private static final String CONF_BATCH_SIZE = "batchSize";
    private static final String CONF_COLUMN_DEFINITION_FILE = "definitionFile";
    private static final String CONF_CONSISTENCY_LEVEL = "consistency";
    private static final String CONF_KEYSPACE_STATEMENT = "keyspaceStatement";
	private static final String CONF_TABLE_STATEMENT = "tableStatement";

    private SinkCounter sinkCounter;
    private CassandraRepository repository;
    private int batchsize;
    private EventParser parser;

    public CassandraSink() {
        super();
    }

    CassandraSink(Channel channel, SinkCounter sinkCounter,
            CassandraRepository repository, int batchSize, EventParser parser) {
        setChannel(channel);
        this.sinkCounter = sinkCounter;
        this.batchsize = batchSize;
        this.repository = repository;
        this.parser = parser;
    }

    @Override
    public void configure(Context context) {
        String table = context.getString(CONF_TABLE);
        String host = context.getString(CONF_HOST);
        String keyspace = context.getString(CONF_KEYSPACE);
        int port = context.getInteger(CONF_PORT);
        String clusterName = context.getString(CONF_CLUSTER);
        String consistency = context.getString(
        		CONF_CONSISTENCY_LEVEL, CASSANDRA_DEFAULT_CONSISTENCY);
        String columnDefinitionFile = context.getString(CONF_COLUMN_DEFINITION_FILE);
        this.parser = new EventParser(readJsonFromFile(new File(columnDefinitionFile)));
        this.repository = new CassandraRepository(host, table, keyspace,
                port, clusterName, consistency, this.parser.getDefinition());
        setOptionalRepoConfiguration(context);
        
        this.batchsize = context.getInteger(CONF_BATCH_SIZE, CASSANDRA_DEFAULT_BATCH_SIZE);
        this.sinkCounter = new SinkCounter(this.getName());
    }
    
    private void setOptionalRepoConfiguration(Context context) {
    	String primaryKey = context.getString(CONF_PRIMARY_KEY);
    	String keyspaceStatement = context.getString(CONF_KEYSPACE_STATEMENT);
    	String tableStatement = context.getString(CONF_TABLE_STATEMENT);
    	
    	this.repository.setPrimaryKey(primaryKey);
    	this.repository.setKeyspaceStatement(keyspaceStatement);
    	this.repository.setTableStatement(tableStatement);
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
                List<CassandraRow> rows = this.parser.parse(eventList);
                this.repository.save(rows);
                this.sinkCounter.addToEventDrainSuccessCount(eventList.size());
            } else {
                this.sinkCounter.incrementBatchEmptyCount();
            }
            transaction.commit();
            status = Status.READY;
        } catch (ChannelException e) {
            e.printStackTrace();
            transaction.rollback();
            status = Status.BACKOFF;
            this.sinkCounter.incrementConnectionFailedCount();
        } catch (Throwable t) {
            t.printStackTrace();
            transaction.rollback();
            status = Status.BACKOFF;
            if (t instanceof Error) {
                throw new CassandraSinkException(t);
            }
        } finally {
            transaction.close();
        }
        return status;
    }

    @Override
    public synchronized void start() {
    	this.repository.createStructure();
        this.sinkCounter.start();
        super.start();
    }

    @Override
    public synchronized void stop() {
        this.repository.close();
        this.sinkCounter.stop();
        super.stop();
    }

    CassandraRepository getRepository() {
        return this.repository;
    }

    private List<Event> takeEventsFromChannel(Channel channel, int eventsToTake) {
        List<Event> events = new ArrayList<Event>();
        for (int i = 0; i < this.batchsize; i++) {
            this.sinkCounter.incrementEventDrainAttemptCount();
            events.add(channel.take());
        }
        events.removeAll(Collections.singleton(null));
        return events;
    }

    private static final String readJsonFromFile(File file) {
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(file);
            return IOUtils.toString(inputStream, "UTF-8");
        } catch (Exception e) {
            throw new CassandraSinkException(e);
        }
    }
}
