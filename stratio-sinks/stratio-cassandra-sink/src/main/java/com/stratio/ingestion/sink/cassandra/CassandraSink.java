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

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Strings;

public class CassandraSink extends AbstractSink implements Configurable {

	private static final String DEFAULT_TABLE = "table_logs";
    private static final String DEFAULT_KEYSPACE = "keyspace_logs";
    private static final int DEFAULT_PORT = 9042;
    private static final String DEFAULT_CLUSTER = "TestCluster";
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final String DEFAULT_CONSISTENCY_LEVEL = "QUORUM";
	
	private static final String DEFAULT_DATE_FORMAT = "dd/MM/yyyy";
	private static final String DEFAULT_ITEM_SEPARATOR = ",";
	private static final String DEFAULT_MAP_VALUE_SEPARATOR = ";";
	private static final String DEFAULT_MAP_KEY_TYPE = "TEXT";
	private static final String DEFAULT_MAP_VALUE_TYPE = "INT";
	private static final String DEFAULT_LIST_VALUE_TYPE = "TEXT";
	
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
	
	private static final String CONF_DATE_FORMAT = "dateFormat";
    private static final String CONF_ITEM_SEPARATOR = "itemSeparator";
    private static final String CONF_MAP_VALUE_SEPARATOR = "mapValueSeparator";
    private static final String CONF_MAP_KEY_TYPE = "mapKeyType";
    private static final String CONF_MAP_VALUE_TYPE = "mapValueType";
	private static final String CONF_LIST_VALUE_TYPE = "listValueType";

    private SinkCounter sinkCounter;
    private CassandraRepository repository;
    private int batchsize;
    private EventParser parser;
    private String dateFormat;
    private String itemSeparator;
    private String mapValueSeparator;
    private String mapKeyType;
    private String mapValueType;
    private String listValueType;

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
        String table = context.getString(CONF_TABLE, DEFAULT_TABLE);
        String host = context.getString(CONF_HOST, DEFAULT_HOST);
        String keyspace = context.getString(CONF_KEYSPACE, DEFAULT_KEYSPACE);
        int port = context.getInteger(CONF_PORT, DEFAULT_PORT);
        String clusterName = context.getString(CONF_CLUSTER, DEFAULT_CLUSTER);
        String consistency = context.getString(
        		CONF_CONSISTENCY_LEVEL, DEFAULT_CONSISTENCY_LEVEL);
        String columnDefinitionFile = context.getString(CONF_COLUMN_DEFINITION_FILE);
        if (!Strings.isNullOrEmpty(columnDefinitionFile)) {
        	this.parser = new EventParser(readJsonFromFile(new File(columnDefinitionFile)));
        }
        ColumnDefinition definition = this.parser == null ? null : this.parser.getDefinition();
        this.repository = new CassandraRepository(host, table, keyspace,
                port, clusterName, consistency, definition);
        setOptionalRepoConfiguration(context);
        
        this.batchsize = context.getInteger(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE);
        this.sinkCounter = new SinkCounter(this.getName());
        
        this.dateFormat = context.getString(CONF_DATE_FORMAT, DEFAULT_DATE_FORMAT);
        this.itemSeparator = context.getString(CONF_ITEM_SEPARATOR, DEFAULT_ITEM_SEPARATOR);
        this.mapValueSeparator = context.getString(CONF_MAP_VALUE_SEPARATOR, DEFAULT_MAP_VALUE_SEPARATOR);
        this.mapKeyType = context.getString(CONF_MAP_KEY_TYPE, DEFAULT_MAP_KEY_TYPE);
        this.mapValueType = context.getString(CONF_MAP_VALUE_TYPE, DEFAULT_MAP_VALUE_TYPE);
        this.listValueType = context.getString(CONF_LIST_VALUE_TYPE, DEFAULT_LIST_VALUE_TYPE);
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
    	TableMetadata table = this.repository.createStructure();
    	if (parser == null) {
    		parser = new EventParser(getColumnDefinition(table));
    	}
        this.sinkCounter.start();
        super.start();
    }
    
	private ColumnDefinition getColumnDefinition(TableMetadata tableMetadata) {
		List<FieldDefinition> fields = new ArrayList<>();
		for (ColumnMetadata column : tableMetadata.getColumns()) {
			FieldDefinition field = new FieldDefinition();
			field.setColumnName(column.getName());
			field.setType(column.getType().getName().name().toUpperCase());
			field.setDateFormat(dateFormat);
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
        for (int i = 0; i < eventsToTake; i++) {
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
