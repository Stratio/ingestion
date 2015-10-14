/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.ingestion.sink.mongodb;

import com.mongodb.*;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 * Reads events from a channel and writes them to MongoDB. It can read fields
 * from both body and headers.
 *
 * Configuration parameters are:
 *
 * <p><ul>
 * <li><tt>dynamic</tt> <em>(boolean)</em>: If true, the dynamic mode will be
 *      enabled and the database and collection to use will be selected by the
 *      event headers. Defaults to <tt>false</tt>.</li>
 * <li><tt>dynamicDB</tt> <em>(string)</em>: Name of the event header that will
 *      be looked up for the database name. This will only work when dynamic
 *      mode is enabled. Defaults to <tt>db</tt>.</li>
 * <li><tt>dynamicCollection</tt> <em>(string)</em>: Name of the event header
 *      that will be looked up for the collection name. This will only work when
 *      dynamic mode is enabled. Defaults to <tt>collection</tt>.</li>
 * <li><tt>mongoUri</tt> <em>(string, required)</em>:
 *      A <a href="http://api.mongodb.org/java/current/com/mongodb/MongoClientURI.html">Mongo client URI</a>
 *      defining the MongoDB server address and, optionally, default database
 *      and collection. When dynamic mode is enabled, the collection defined
 *      here will be used as a fallback.</li>
 * <li><tt>mappingFile</tt> <em>(string)</em>: Path to a
 *      <a href="http://json-schema.org/">JSON Schema</a>
 *      to be used for type mapping purposes.</li>
 * </ul></p>
 *
 */
public class MongoSink extends AbstractSink implements Configurable {

    private static final Logger log = LoggerFactory.getLogger(MongoSink.class);

    private static final String CONF_URI = "mongoUri";
    private static final String CONF_MAPPING_FILE = "mappingFile";
    private static final String CONF_BATCH_SIZE = "batchSize";
    private static final String CONF_DYNAMIC = "dynamic";
    private static final String CONF_DYNAMIC_DB_FIELD = "dynamicDB";
    private static final String CONF_DYNAMIC_COLLECTION_FIELD = "dynamicCollection";
    private static final int DEFAULT_BATCH_SIZE = 25;
    private static final boolean DEFAULT_DYNAMIC = false;
    private static final String DEFAULT_DYNAMIC_DB_FIELD = "db";
    private static final String DEFAULT_DYNAMIC_COLLECTION_FIELD = "collection";

    private SinkCounter sinkCounter;
    private int batchSize;
    private MongoClient mongoClient;
    private MongoClientURI mongoClientURI;
    private DB mongoDefaultDb;
    private DBCollection mongoDefaultCollection;
    private boolean isDynamicMode;
    private String dynamicDBField;
    private String dynamicCollectionField;
    private EventParser eventParser;

    public MongoSink() {
        super();
    }

    /**
     * {@inheritDoc}
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        try {
            if (!"INJECTED".equals(context.getString(CONF_URI))) {
                this.mongoClientURI = new MongoClientURI(
                        context.getString(CONF_URI),
                        MongoClientOptions.builder().writeConcern(WriteConcern.SAFE)
                );
                this.mongoClient = new MongoClient(mongoClientURI);
                if (mongoClientURI.getDatabase() != null) {
                    this.mongoDefaultDb = mongoClient.getDB(mongoClientURI.getDatabase());
                }
                if (mongoClientURI.getCollection() != null) {
                    this.mongoDefaultCollection = mongoDefaultDb.getCollection(mongoClientURI.getCollection());
                }
            }

            final String mappingFilename = context.getString(CONF_MAPPING_FILE);
            this.eventParser = (mappingFilename == null) ?
                    new EventParser()
                    :
                    new EventParser(MappingDefinition.load(mappingFilename));

            this.isDynamicMode = context.getBoolean(CONF_DYNAMIC, DEFAULT_DYNAMIC);
            if (!isDynamicMode && mongoDefaultCollection == null) {
                throw new MongoSinkException("Default MongoDB collection must be specified unless dynamic mode is enabled");
            }
            this.dynamicDBField = context.getString(CONF_DYNAMIC_DB_FIELD, DEFAULT_DYNAMIC_DB_FIELD);
            this.dynamicCollectionField = context.getString(CONF_DYNAMIC_COLLECTION_FIELD, DEFAULT_DYNAMIC_COLLECTION_FIELD);

            this.sinkCounter = new SinkCounter(this.getName());
            this.batchSize = context.getInteger(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE);
        } catch (IOException ex) {
            throw new MongoSinkException(ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.BACKOFF;
        Transaction transaction = this.getChannel().getTransaction();
        log.debug("Executing MongoSink.process");
        try {
            transaction.begin();
            List<Event> eventList = this.takeEventsFromChannel(
                    this.getChannel(), this.batchSize);
            status = Status.BACKOFF;
            if (!eventList.isEmpty()) {
                if (eventList.size() == this.batchSize) {
                    this.sinkCounter.incrementBatchCompleteCount();
                } else {
                    this.sinkCounter.incrementBatchUnderflowCount();
                }
                for (Event event : eventList) {
                    final DBObject document = this.eventParser.parse(event);
                    getDBCollection(event).save(document);
                }
                this.sinkCounter.addToEventDrainSuccessCount(eventList.size());
            } else {
                this.sinkCounter.incrementBatchEmptyCount();
            }
            transaction.commit();
            status = Status.READY;
        } catch (ChannelException e) {
            log.error("Unexpected error while executing MongoSink.process", e);
            transaction.rollback();
            status = Status.BACKOFF;
            this.sinkCounter.incrementConnectionFailedCount();
        } catch (Throwable t) {
            log.error("Unexpected error while executing MongoSink.process", t);
            transaction.rollback();
            status = Status.BACKOFF;
            if (t instanceof Error) {
                throw new MongoSinkException(t);
            }
        } finally {
            transaction.close();
        }
        return status;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void start() {
        this.sinkCounter.start();
        super.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void stop() {
        this.mongoClient.close();
        this.sinkCounter.stop();
        super.stop();
    }

    private DBCollection getDBCollection(Event event) {
        if (!isDynamicMode) {
            return mongoDefaultCollection;
        }
        final Map<String, String> headers = event.getHeaders();
        final String dbName = headers.get(dynamicDBField);
        final String collectionName = headers.get(dynamicCollectionField);
        if (collectionName == null) {
            if (mongoDefaultCollection == null) {
                throw new MongoSinkException("No collection specified and no default set");
            }
            return mongoDefaultCollection;
        }
        DB db;
        if (dbName == null) {
            if (mongoDefaultDb == null) {
                throw new MongoSinkException("No DB specified and no default set");
            }
            db = mongoDefaultDb;
        } else {
            db = mongoClient.getDB(dbName);
        }
        return db.getCollection(collectionName);
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
