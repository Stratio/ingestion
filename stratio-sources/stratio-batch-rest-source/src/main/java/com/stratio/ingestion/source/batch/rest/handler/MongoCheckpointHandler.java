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
package com.stratio.ingestion.source.batch.rest.handler;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.UnknownHostException;

import org.apache.flume.Context;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import com.stratio.ingestion.source.batch.rest.MongoSourceException;
import com.stratio.ingestion.source.batch.rest.checkpoint.CheckpointType;

/**
 * Created by eambrosio on 30/12/14.
 */
public class MongoCheckpointHandler extends CheckpointHandler {

    protected static final String MONGO_URI = "mongoURI";
    protected static final String CONF_CHECKPOINT_FIELD = "checkpointField";
    protected static final String CONF_CHECKPOINT_TYPE = "checkpointType";

    private MongoClient mongoClient;
    private MongoClientURI mongoClientURI;
    private DB mongoDb;
    private DBCollection mongoCollection;
    private String checkpointField;
    private CheckpointType checkpointType;

    public MongoCheckpointHandler(Context context, CheckpointType checkpointType) {
        this.checkpointField = checkNotNull(context.getString(CONF_CHECKPOINT_FIELD), "Expected non-null"
                + " checkpoint field");
        this.checkpointType = checkNotNull(checkpointType, "Expected non-null checkpoint type");

        this.mongoClientURI = new MongoClientURI(
                context.getString(MONGO_URI),
                MongoClientOptions.builder().writeConcern(WriteConcern.SAFE));
        try {
            this.mongoClient = new MongoClient(mongoClientURI);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        if (mongoClientURI.getDatabase() != null) {
            this.mongoDb = mongoClient.getDB(mongoClientURI.getDatabase());
        }
        if (mongoClientURI.getCollection() != null) {
            this.mongoCollection = mongoDb.getCollection(mongoClientURI.getCollection());
        }
    }

    @Override
    public String getLastCheckpoint(Context context) {
        String checkpoint;
        DBCursor cursor;
        Object field = null;
        final long count = mongoCollection.count();
        if (count > 0) {
            try {

                cursor = mongoCollection.find().skip((int) (count - 1));
                while (cursor.hasNext()) {
                    DBObject object = cursor.next();
                    field = object.get(checkpointField);
                }
                checkpoint = checkpointType.populateCheckpoint(field, context);

            } catch (Exception e) {
                throw new MongoSourceException("Error accesing DB. Verify db/collection name.");
            }
        } else {
            checkpoint = checkpointType.populateDefaultCheckpoint(context);
        }

        return checkpoint;
    }

}
