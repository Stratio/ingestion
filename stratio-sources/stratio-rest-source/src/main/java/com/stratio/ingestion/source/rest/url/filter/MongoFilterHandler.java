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
    public Map<String, String> getLastFilter(Map<String, String> context) {
        String filter;
        DBCursor cursor;
        Object fieldValue = null;
        final long count = countFilters();
        if (count > 0) {
            try {
                cursor = mongoCollection.find().skip((int) (count - 1));
                while (cursor.hasNext()) {
                    DBObject object = cursor.next();
                    fieldValue = object.get(filterField);
                }
                filter = (String) filterType.buildFilter(fieldValue, context);

            } catch (Exception e) {
                throw new MongoFilterException("Error accesing DB. Verify db/collection name.");
            }
        } else {
            filter = (String) filterType.buildDefaultFilter(context);
        }
        return ImmutableMap.<String, String>builder().put(filterField, filter).build();
    }

    @Override public void updateFilter(String filter) {
        ObjectMapper mapper = new ObjectMapper();
        if (StringUtils.isNotEmpty(filter)) {
            try {
                final HashMap filterMap = mapper.readValue(filter, HashMap.class);
                DBObject object = new BasicDBObject();
                object.put(filterField, filterType.parseFilter(filterMap.get(filterField), context));
                saveDocument(object);
            } catch (JsonMappingException e) {
                throw new MongoFilterException("An error occurred while mapping filter value to Mongo", e);
            } catch (JsonParseException e) {
                throw new MongoFilterException("An error occurred while parsing filter value to json", e);
            } catch (IOException e) {
                throw new MongoFilterException("An error occurred while updating filter value to Mongo", e);
            } catch (ParseException e) {
                throw new MongoFilterException("An error occurred while parsing filter value to Mongo", e);
            }
        }

    }

    protected void saveDocument(DBObject object) {
        mongoCollection.save(object);
    }

    private long countFilters() {
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
            filterType = (CheckpointType) Class.forName(context.get("filterType")).newInstance();
            filterField = checkNotNull(context.get("field"), "Expected non-null filterField");
        } catch (Exception e) {
            throw new MongoFilterException("An error occurred during filterType instantiation", e);
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
        Map<String, String> checkpointContext = new HashMap<String, String>();
        final JsonNode jsonNode = loadConfigurationFile(context.get(FILTER_CONF));
        checkpointContext.put("field", checkNotNull(jsonNode.findValue("field").asText(), "Non-null field value "
                + "expected"));
        checkpointContext.put("mongoUri", checkNotNull(jsonNode.findValue("mongoUri").asText(), "Non-null mongoUri "
                + "value expected"));
        checkpointContext.put("filterType", checkNotNull(jsonNode.findValue("type").asText(), "Non-null type "
                + "value expected"));
        checkpointContext.put("dateFormat", checkNotNull(jsonNode.findValue("dateFormat").asText(), "Non-null "
                + "dateFormat value expected"));
        return checkpointContext;
    }

    protected JsonNode loadConfigurationFile(String jsonFile) {
        JsonNode jsonNode = null;
        if (StringUtils.isNotBlank(jsonFile)) {
            try {
                File checkpointFile = new File(jsonFile);
                if (checkpointFile.exists()) {
                    ObjectMapper mapper = new ObjectMapper();
                    jsonNode = mapper.readTree(checkpointFile);
                } else {
                    throw new RestSourceException("The configuration file doesn't exist");
                }
            } catch (Exception e) {
                throw new RestSourceException("An error ocurred while json parsing. Verify configuration  file", e);
            }
        }
        return jsonNode;
    }

    protected void initMongo(String mongoUri) {
        try {
            this.mongoClientURI = new MongoClientURI(mongoUri,
                    MongoClientOptions.builder().writeConcern(WriteConcern.SAFE));
            this.mongoClient = new MongoClient(mongoClientURI);
            this.mongoDb = mongoClient.getDB(checkNotNull(mongoClientURI.getDatabase(), "Non-null database expected"));
            this.mongoCollection = mongoDb.getCollection(checkNotNull(mongoClientURI.getCollection(), "Non-null "
                    + "collection expected"));
        } catch (UnknownHostException e) {
            throw new MongoFilterException("mongo host could not be reached", e);
        } catch (IllegalArgumentException e) {
            throw new MongoFilterException("Valid mongo uri expected.", e);
        } catch (NullPointerException e) {
            throw new MongoFilterException("Non-null db/collection expected", e);
        }
    }
}
