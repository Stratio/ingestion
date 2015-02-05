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
package com.stratio.ingestion.source.rest.url.filter;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.stratio.ingestion.source.rest.url.filter.exception.MongoCheckpointFilterException;
import com.stratio.ingestion.source.rest.url.filter.type.CheckpointType;

/**
 * Created by eambrosio on 14/01/15.
 */
public class MongoCheckpointFilterHandler extends CheckpointFilterHandler {

    protected static final String MONGO_URI = "mongoUri";
    private MongoClient mongoClient;
    private MongoClientURI mongoClientURI;
    private DB mongoDb;
    private DBCollection mongoCollection;

    public MongoCheckpointFilterHandler(CheckpointType checkpointType, Map<String, String> context) {
        super(checkpointType, context);
        initMongo(checkNotNull(context.get(MONGO_URI), "Expected non-null mongoUri field"));
    }

    private void initMongo(String mongoUri) {
        this.mongoClientURI = new MongoClientURI(
                mongoUri, MongoClientOptions.builder().writeConcern(WriteConcern.SAFE));
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
    public String getLastCheckpoint(Map<String, String> context) {
        String checkpoint;
        DBCursor cursor;
        Object fieldValue = null;
        final long count = countCheckpoints();
        if (count > 0) {
            try {
                cursor = mongoCollection.find().skip((int) (count - 1));
                while (cursor.hasNext()) {
                    DBObject object = cursor.next();
                    fieldValue = object.get(checkpointField);
                }
                checkpoint = (String) checkpointType.buildCheckpoint(fieldValue, context);

            } catch (Exception e) {
                throw new MongoCheckpointFilterException("Error accesing DB. Verify db/collection name.");
            }
        } else {
            checkpoint = (String) checkpointType.buildDefaultCheckpoint(context);
        }
        return checkpoint;
    }

    @Override public void updateCheckpoint(String checkpoint) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            final HashMap checkpointMap = mapper.readValue(checkpoint, HashMap.class);
            DBObject object = new BasicDBObject();
            object.put(checkpointField, checkpointType.parseCheckpoint(checkpointMap.get(checkpointField), context));
            saveDocument(object);
        } catch (JsonMappingException e) {
            throw new MongoCheckpointFilterException("An error occurred while mapping checkpoint value to Mongo", e);
        } catch (JsonParseException e) {
            throw new MongoCheckpointFilterException("An error occurred while parsing checkpoint value to json", e);
        } catch (IOException e) {
            throw new MongoCheckpointFilterException("An error occurred while updating checkpoint value to Mongo", e);
        } catch (ParseException e) {
            throw new MongoCheckpointFilterException("An error occurred while parsibg checkpoint value to Mongo", e);
        }

    }

    private Object formatCheckpointField(HashMap checkpointMap) {
        try {
            return new SimpleDateFormat(context.get("format")).parse((String) checkpointMap.get(checkpointField));
        } catch (ParseException e) {
            throw new MongoCheckpointFilterException("An error occurred while parsing checkpoint field.", e);
        }
    }

    protected void saveDocument(DBObject object) {
        mongoCollection.save(object);
    }

    private long countCheckpoints() {
        long count = 0;
        try {
            count = mongoCollection.count();
        } catch (MongoException e) {
            throw new MongoCheckpointFilterException("An error ocurred while connecting to DB. Verify DB status.", e);
        }
        return count;
    }

}
