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

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.stratio.ingestion.source.rest.url.filter.exception.MongoFilterException;

public class MongoFilterHandlerTestIT {

    public static final String DB_TEST = "test_MongoCheckpointFilterHandlerIT";
    public static final String DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    private static final String MONGO_URI = "mongoUri";
    private MongoFilterHandler handler;

//    @Mock
    private Map<String, String> context = mock(HashMap.class);

    private MongoClient mongoClient;

    public static String getMongoHost() {
        String mongoIp = System.getProperty("mongo.hosts.0").split(":")[0];
        if (mongoIp == null) {
            mongoIp = "127.0.0.1";
        }
        String mongoPort = "27017";
        if(System.getProperty("mongo.hosts.0").length() > 1) {
            mongoPort = System.getProperty("mongo.hosts.0").split(":")[1];
        }
//        String mongoIp = System.getProperty("mongo.ip");
//        if (mongoIp == null) {
//            mongoIp = "127.0.0.1";
//        }
//        String mongoPort = System.getProperty("mongo.port");
//        if (mongoPort == null) {
//            mongoPort = "27017";
//        }
        return mongoIp + ":" + mongoPort;
    }

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);
    }

    @Test(expected = NullPointerException.class)
    public void updateCheckpointWithNoValidCheckpointField() throws Exception {
        String mongoHost = getMongoHost();
        String checkpoint = "{\"date\":\"01-01-01\"}";
        when(context.get(MONGO_URI)).thenReturn("mongodb://" + mongoHost + "/noExistingDB.profiles");

        handler = new MongoFilterHandler();
        handler.updateFilter(checkpoint);
    }

    @Test(expected = NullPointerException.class)
    public void updateCheckpointWithNoValidCheckpointType() throws Exception {
        String mongoHost = getMongoHost();
        String checkpoint = "{\"date\":\"01-01-01\"}";
        when(context.get(MONGO_URI)).thenReturn("mongodb://" + mongoHost + "/noExistingDB.profiles");
        when(context.get("field")).thenReturn("date");

        handler = new MongoFilterHandler();
        handler.updateFilter(checkpoint);
    }

    @Test
    public void getLastCheckpointWithNoExistingDB() throws Exception {
        String mongoHost = getMongoHost();
        when(context.get("field"))
                .thenReturn("validName");
        when(context.get("type"))
                .thenReturn("java.util.Date");
        when(context.get("format"))
                .thenReturn(DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX);
        when(context.get("mongoUri"))
                .thenReturn("mongodb://" + mongoHost + "/noExistingDB.emptyCollection");
        when(context.get("filterType"))
                .thenReturn("com.stratio.ingestion.source.rest.url.filter.type.DateCheckpointType");
        handler = spy(new MongoFilterHandler());
        doReturn(context).when(handler).loadCheckpointContext(context);
        handler.configure(context);

        final Map<String, String> lastCheckpoint = handler.getLastFilter(context);
        assertThat(lastCheckpoint).isNotNull();
        assertThat(lastCheckpoint.get("validName"))
                .isEqualTo(new SimpleDateFormat(DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX).format(new Date(0)));
    }

    @Test
    public void getLastCheckPointWithEmptyCollection() throws UnknownHostException {
        String mongoHost = getMongoHost();
        when(context.get("field"))
                .thenReturn("validName");
        when(context.get("type"))
                .thenReturn("java.util.Date");
        when(context.get("format"))
                .thenReturn(DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX);
        when(context.get("mongoUri"))
                .thenReturn("mongodb://" + mongoHost + "/" + DB_TEST + ".emptyCollection");
        handler = new MongoFilterHandler();
        when(context.get("filterType"))
                .thenReturn("com.stratio.ingestion.source.rest.url.filter.type.DateCheckpointType");
        handler = spy(new MongoFilterHandler());
        doReturn(context).when(handler).loadCheckpointContext(context);
        handler.configure(context);

        final Map<String, String> lastCheckpoint = handler.getLastFilter(context);
        assertThat(lastCheckpoint).isNotNull();
        assertThat(lastCheckpoint.get("validName"))
                .isEqualTo(new SimpleDateFormat(DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX).format(new Date(0)));
    }

    @Test
    public void getLastCheckPointWithValidCollection() throws UnknownHostException, ParseException {
        String mongoHost = getMongoHost();
        when(context.get("field"))
                .thenReturn("date");
        when(context.get("type"))
                .thenReturn("java.util.Date");
        when(context.get("format"))
                .thenReturn(DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX);
        when(context.get("mongoUri"))
                .thenReturn("mongodb://" + mongoHost + "/" + DB_TEST + ".validCollection");
        handler = new MongoFilterHandler();
        mongoClient = new MongoClient(new MongoClientURI("mongodb://" + mongoHost));
        mongoClient.getDB(DB_TEST).createCollection("validCollection", null);
        mongoClient.getDB(DB_TEST).getCollection("validCollection").save(populateDocument());
        when(context.get("filterType"))
                .thenReturn("com.stratio.ingestion.source.rest.url.filter.type.DateCheckpointType");
        handler = spy(new MongoFilterHandler());
        doReturn(context).when(handler).loadCheckpointContext(context);
        handler.configure(context);

        final Map<String, String> lastCheckpoint = handler.getLastFilter(context);
        assertThat(lastCheckpoint).isNotNull();
        assertThat(lastCheckpoint.get("date")).isEqualTo("2014-12-16T16:32:33.000+0100");
    }

    @Test
    public void updateCheckpoint() throws Exception {
        String mongoHost = getMongoHost();
        when(context.get("field"))
                .thenReturn("date");
        when(context.get("type"))
                .thenReturn("java.util.Date");
        when(context.get("format"))
                .thenReturn(DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX);
        when(context.get("mongoUri"))
                .thenReturn("mongodb://" + mongoHost + "/" + DB_TEST + ".validCollection");
//        when(context.get("mongoUri"))
//                .thenReturn("mongodb://127.0.0.1:27017/" + DB_TEST + ".validCollection");
        handler = spy(new MongoFilterHandler());
        when(context.get("filterType"))
                .thenReturn("com.stratio.ingestion.source.rest.url.filter.type.DateCheckpointType");
        doReturn(context).when(handler).loadCheckpointContext(context);
        handler.configure(context);

        handler.updateFilter(
                "{\"date\":\"2015-02-04T16:10:00.000+0100\",\"santanderId\":\"1234567890\"}");

        verify(handler).saveDocument(any(DBObject.class));
        assertThat(handler.getLastFilter(context).get("date")).isEqualToIgnoringCase("2015-02-04T16:10:00"
                + ".000+0100");
    }

    @Test(expected = MongoFilterException.class)
    public void updateCheckpointWithInvalidDateFormat() throws Exception {
        String mongoHost = getMongoHost();
        when(context.get("field"))
                .thenReturn("date");
        when(context.get("type"))
                .thenReturn("java.util.Date");
        when(context.get("format"))
                .thenReturn(DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX);
        when(context.get("mongoUri"))
                .thenReturn("mongodb://" + mongoHost + "/" + DB_TEST + ".validCollection");
        handler = spy(new MongoFilterHandler());
        when(context.get("filterType"))
                .thenReturn("com.stratio.ingestion.source.rest.url.filter.type.DateCheckpointType");
        doReturn(context).when(handler).loadCheckpointContext(context);
        handler.configure(context);

        handler.updateFilter(
                "{\"date\":\"2015-02-04T16:10:00+0100\",\"santanderId\":\"1234567890\"}");
    }

    @Test(expected = MongoFilterException.class)
    public void updateCheckpointWithMalFormedJson() throws Exception {
        String mongoHost = getMongoHost();
        when(context.get("field"))
                .thenReturn("date");
        when(context.get("type"))
                .thenReturn("java.util.Date");
        when(context.get("format"))
                .thenReturn(DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX);
        when(context.get("mongoUri"))
                .thenReturn("mongodb://" + mongoHost + "/" + DB_TEST + ".validCollection");
        handler = spy(new MongoFilterHandler());
        when(context.get("filterType"))
                .thenReturn("com.stratio.ingestion.source.rest.url.filter.type.DateCheckpointType");
        doReturn(context).when(handler).loadCheckpointContext(context);
        handler.configure(context);

        handler.updateFilter(
                "{\"date\":,\"santanderId\":\"1234567890\"}");
    }

    @Test(expected = NullPointerException.class)
    public void loadCheckpointFilterWithNoValidProperties() throws IOException {
        handler = spy(new MongoFilterHandler());
        JsonNode jsonObject = new ObjectMapper().readTree(
                "{\"field\":\"date\"}");
        doReturn(jsonObject).when(handler).loadConfigurationFile(anyString());

        handler.loadCheckpointContext(context);
    }

    @Test
    public void loadCheckpointFilterWithTooMuchProperties() throws IOException {
        handler = spy(new MongoFilterHandler());
        JsonNode jsonObject = new ObjectMapper().readTree(
                "{\"field\":\"date\","
                        + "\"type\":\"com.stratio.ingestion.source.rest.url.filter.type.DateCheckpointType\","
                        + "\"dateFormat\":\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\","
                        + "\"dateFormat2\":\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\","
                        + "\"dateFormat3\":\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\","
                        + "\"mongoUri\":\"mongodb://127.0.0.1:27017/socialLogin.checkpoints\"}");
        doReturn(jsonObject).when(handler).loadConfigurationFile(anyString());

        final Map<String, String> filterContext = handler.loadCheckpointContext(context);
        assertThat(filterContext).isNotEmpty().hasSize(4);
    }

    @Test
    public void loadCheckpointFilterWithValidProperties() throws IOException {
        handler = spy(new MongoFilterHandler());
        JsonNode jsonObject = new ObjectMapper().readTree(
                "{\"field\":\"date\","
                        + "\"type\":\"com.stratio.ingestion.source.rest.url.filter.type.DateCheckpointType\","
                        + "\"dateFormat\":\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\","
                        + "\"mongoUri\":\"mongodb://127.0.0.1:27017/socialLogin.checkpoints\"}");
        doReturn(jsonObject).when(handler).loadConfigurationFile(anyString());

        final Map<String, String> filterContext = handler.loadCheckpointContext(context);
        assertThat(filterContext).isNotEmpty().hasSize(4);
    }

    @Test(expected = MongoFilterException.class)
    public void initMongoWithEmptyDB(){
        handler = new MongoFilterHandler();
        handler.initMongo("mongodb://127.0.0.1:27017/");

    }
    
    @Test(expected = MongoFilterException.class)
     public void initMongoWithEmptyCollection() {
        handler = new MongoFilterHandler();
        handler.initMongo("mongodb://127.0.0.1:27017/db");

    }

    @Test(expected = MongoFilterException.class)
    public void initMongoWithEmptyMongoUri()  {
        handler = new MongoFilterHandler();
        handler.initMongo("");

    }

    private DBObject populateDocument() throws ParseException {
        final DBObject document = new BasicDBObject();
        document.put("santanderID", "documentID");
        document.put("date", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse("2014-12-16T16:32:33"));
        return document;
    }
}