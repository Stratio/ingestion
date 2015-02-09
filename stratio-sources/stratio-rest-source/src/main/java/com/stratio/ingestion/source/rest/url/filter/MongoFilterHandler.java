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

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.ImmutableMap;
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
import com.stratio.ingestion.source.rest.exception.RestSourceException;
import com.stratio.ingestion.source.rest.url.filter.exception.MongoFilterException;
import com.stratio.ingestion.source.rest.url.filter.type.CheckpointType;

/**
 * Created by eambrosio on 14/01/15.
 */
public class MongoFilterHandler extends FilterHandler {

    private static final String MONGO_URI = "mongoUri";
    private static final String FILTER_CONF = "filterConfiguration";
    private MongoClient mongoClient;
    private MongoClientURI mongoClientURI;
    private DB mongoDb;
    private DBCollection mongoCollection;

    @Override
    public Map<String,String> getLastCheckpoint(Map<String, String> context) {
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
                throw new MongoFilterException("Error accesing DB. Verify db/collection name.");
            }
        } else {
            checkpoint = (String) checkpointType.buildDefaultCheckpoint(context);
        }
        return ImmutableMap.<String,String>builder().put(checkpointField,checkpoint).build();
    }

    @Override public void updateCheckpoint(String checkpoint) {
        ObjectMapper mapper = new ObjectMapper();
        if (StringUtils.isNotEmpty(checkpoint)) {
            try {
                final HashMap checkpointMap = mapper.readValue(checkpoint, HashMap.class);
                DBObject object = new BasicDBObject();
                object.put(checkpointField,
                        checkpointType.parseCheckpoint(checkpointMap.get(checkpointField), context));
                saveDocument(object);
            } catch (JsonMappingException e) {
                throw new MongoFilterException("An error occurred while mapping checkpoint value to Mongo",
                        e);
            } catch (JsonParseException e) {
                throw new MongoFilterException("An error occurred while parsing checkpoint value to json", e);
            } catch (IOException e) {
                throw new MongoFilterException("An error occurred while updating checkpoint value to Mongo",
                        e);
            } catch (ParseException e) {
                throw new MongoFilterException("An error occurred while parsing checkpoint value to Mongo",
                        e);
            }
        }

    }

    private Object formatCheckpointField(HashMap checkpointMap) {
        try {
            return new SimpleDateFormat(context.get("format")).parse((String) checkpointMap.get(checkpointField));
        } catch (ParseException e) {
            throw new MongoFilterException("An error occurred while parsing checkpoint field.", e);
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
            throw new MongoFilterException("An error ocurred while connecting to DB. Verify DB status.", e);
        }
        return count;
    }

    @Override public void configure(Map<String, String> contextx) {
        context = loadCheckpointContext(contextx);
        try {
            initMongo(checkNotNull(context.get(MONGO_URI), "Expected non-null mongoUri field"));
            checkpointType = (CheckpointType) Class.forName(context.get("checkpointType")).newInstance();
            checkpointField = checkNotNull(context.get("field"), "Expected non-null checkpoint field");
        } catch (Exception e) {
            throw new MongoFilterException("An error occurred during checkpoint type instantiation", e);
        }
    }

    /**
     * Loads the context configured in the parameter 'checkpointConfiguration'
     *
     * @param context
     * @return
     * @throws Exception
     */
    protected Map<String, String> loadCheckpointContext(Map<String, String> context) {
        Map<String, String> checkpointContext = null;
        JsonNode jsonNode;
        final String string = context.get(FILTER_CONF);

        if (StringUtils.isNotBlank(string)) {
            try {
                File checkpointFile = new File(string);
                if (checkpointFile.exists()) {
                    checkpointContext = new HashMap<String, String>();
                    ObjectMapper mapper = new ObjectMapper();
                    jsonNode = mapper.readTree(checkpointFile);
                    checkpointContext.put("field", jsonNode.findValue("field").asText());
                    checkpointContext.put("mongoUri", jsonNode.findValue("mongoUri").asText());
                    checkpointContext.put("checkpointType", jsonNode.findValue("type").asText());
                    checkpointContext.put("dateFormat", jsonNode.findValue("dateFormat").asText());
                } else {
                    throw new RestSourceException("The checkpoint configuration file doesn't exist");
                }
            } catch (Exception e) {
                throw new RestSourceException("An error ocurred while json parsing. Verify checkpointConfiguration", e);
            }
        }
        return checkpointContext;
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
}
