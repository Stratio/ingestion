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
package com.stratio.ingestion.source.batch.rest.checkpoint;

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

/**
 * Created by eambrosio on 30/12/14.
 */
public class MongoCheckpointRetrieval extends CheckpointRetrieval {

    private static final java.lang.String MONGO_URI = "mongoURI";
    private static final String CONF_CHECKPOINT_FIELD = "checkpointField";
    private static final String CONF_CHECKPOINT_TYPE = "checkpointType";

    private MongoClient mongoClient;
    private MongoClientURI mongoClientURI;
    private DB mongoDb;
    private DBCollection mongoCollection;
    private String checkpointField;
    private String checkpointType;

    public MongoCheckpointRetrieval(Context context) {
        this.checkpointField = context.getString(CONF_CHECKPOINT_FIELD);
        this.checkpointType = context.getString(CONF_CHECKPOINT_TYPE);

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
    public Object getLastCheckpoint() {
        final DBCursor cursor = mongoCollection.find().skip((int) (mongoCollection.count() - 1));
        Object checkpoint = null;
        while (cursor.hasNext()) {
            DBObject object = cursor.next();
            checkpoint = object.get(checkpointField);
        }
        String checkpointAsString = null;
        try {
            checkpoint = Class.forName(checkpointType).cast(checkpoint);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return checkpoint;
    }

}
