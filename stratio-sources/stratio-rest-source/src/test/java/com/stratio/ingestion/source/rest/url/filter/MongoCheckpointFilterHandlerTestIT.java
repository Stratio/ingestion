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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.stratio.ingestion.source.rest.url.filter.exception.MongoCheckpointFilterException;
import com.stratio.ingestion.source.rest.url.filter.type.DateCheckpointType;

public class MongoCheckpointFilterHandlerTestIT {

    public static final String DB_TEST = "test_MongoCheckpointFilterHandlerIT";
    public static final String DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    private static final String MONGO_URI = "mongoUri";
    private MongoCheckpointFilterHandler handler;

    @Mock
    private Map<String, String> context;

    private MongoClient mongoClient;

    public static String getMongoHost() {
        String mongoIp = System.getProperty("mongo.ip");
        if (mongoIp == null) {
            mongoIp = "127.0.0.1";
        }
        String mongoPort = System.getProperty("mongo.port");
        if (mongoPort == null) {
            mongoPort = "27017";
        }
        return mongoIp + ":" + mongoPort;
    }

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);
    }

    @Test(expected = NullPointerException.class)
    public void updateCheckpointWithNoValidCheckpointField() throws Exception {
        String checkpoint = "{\"date\":\"01-01-01\"}";
        when(context.get(MONGO_URI)).thenReturn("mongodb://" + getMongoHost() + "/noExistingDB.profiles");

        handler = new MongoCheckpointFilterHandler(new DateCheckpointType(), context);
        handler.updateCheckpoint(checkpoint);
    }

    @Test(expected = NullPointerException.class)
    public void updateCheckpointWithNoValidCheckpointType() throws Exception {
        String checkpoint = "{\"date\":\"01-01-01\"}";
        when(context.get(MONGO_URI)).thenReturn("mongodb://" + getMongoHost() + "/noExistingDB.profiles");
        when(context.get("field")).thenReturn("date");

        handler = new MongoCheckpointFilterHandler(null, context);
        handler.updateCheckpoint(checkpoint);
    }

    @Test
    public void getLastCheckpointWithNoExistingDB() throws Exception {
        when(context.get("field"))
                .thenReturn("validName");
        when(context.get("type"))
                .thenReturn("java.util.Date");
        when(context.get("format"))
                .thenReturn(DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX);
        when(context.get("mongoUri"))
                .thenReturn("mongodb://" + getMongoHost() + "/noExistingDB.emptyCollection");
        handler = new MongoCheckpointFilterHandler(new DateCheckpointType(), context);
        mongoClient = new MongoClient(new MongoClientURI("mongodb://" + getMongoHost()));
        mongoClient.getDB(DB_TEST).createCollection("emptyCollection", null);

        handler = new MongoCheckpointFilterHandler(new DateCheckpointType(), context);
        final String lastCheckpoint = handler.getLastCheckpoint(context);
        assertThat(lastCheckpoint).isNotNull();
        assertThat(lastCheckpoint)
                .isEqualTo(new SimpleDateFormat(DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX).format(new Date(0)));
    }

    @Test
    public void getLastCheckPointWithEmptyCollection() throws UnknownHostException {
        when(context.get("field"))
                .thenReturn("validName");
        when(context.get("type"))
                .thenReturn("java.util.Date");
        when(context.get("format"))
                .thenReturn(DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX);
        when(context.get("mongoUri"))
                .thenReturn("mongodb://" + getMongoHost() + "/" + DB_TEST + ".emptyCollection");
        handler = new MongoCheckpointFilterHandler(new DateCheckpointType(), context);
        mongoClient = new MongoClient(new MongoClientURI("mongodb://" + getMongoHost()));
        mongoClient.getDB(DB_TEST).createCollection("emptyCollection", null);

        final String lastCheckpoint = handler.getLastCheckpoint(context);
        assertThat(lastCheckpoint).isNotNull();
        assertThat(lastCheckpoint)
                .isEqualTo(new SimpleDateFormat(DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX).format(new Date(0)));
    }

    @Test
    public void getLastCheckPointWithValidCollection() throws UnknownHostException, ParseException {
        when(context.get("field"))
                .thenReturn("date");
        when(context.get("type"))
                .thenReturn("java.util.Date");
        when(context.get("format"))
                .thenReturn(DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX);
        when(context.get("mongoUri"))
                .thenReturn("mongodb://" + getMongoHost() + "/" + DB_TEST + ".validCollection");
        handler = new MongoCheckpointFilterHandler(new DateCheckpointType(), context);
        mongoClient = new MongoClient(new MongoClientURI("mongodb://" + getMongoHost()));
        mongoClient.getDB(DB_TEST).createCollection("validCollection", null);
        mongoClient.getDB(DB_TEST).getCollection("validCollection").save(populateDocument());

        final String lastCheckpoint = handler.getLastCheckpoint(context);
        assertThat(lastCheckpoint).isNotNull();
        assertThat(lastCheckpoint).isEqualTo("2014-12-16T16:32:33.000+0100");
    }

    @Test
    public void updateCheckpoint() throws Exception {
        when(context.get("field"))
                .thenReturn("date");
        when(context.get("type"))
                .thenReturn("java.util.Date");
        when(context.get("format"))
                .thenReturn(DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX);
        when(context.get("mongoUri"))
                .thenReturn("mongodb://" + getMongoHost() + "/" + DB_TEST + ".validCollection");
        handler = spy(new MongoCheckpointFilterHandler(new DateCheckpointType(), context));

        handler.updateCheckpoint(
                "{\"date\":\"2015-02-04T16:10:00.000+0100\",\"santanderId\":\"1234567890\"}");

        verify(handler).saveDocument(any(DBObject.class));
        assertThat(handler.getLastCheckpoint(context)).isEqualToIgnoringCase("2015-02-04T16:10:00.000+0100");
    }

    @Test(expected = MongoCheckpointFilterException.class)
    public void updateCheckpointWithInvalidDateFormat() throws Exception {
        when(context.get("field"))
                .thenReturn("date");
        when(context.get("type"))
                .thenReturn("java.util.Date");
        when(context.get("format"))
                .thenReturn(DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX);
        when(context.get("mongoUri"))
                .thenReturn("mongodb://" + getMongoHost() + "/" + DB_TEST + ".validCollection");
        handler = spy(new MongoCheckpointFilterHandler(new DateCheckpointType(), context));

        handler.updateCheckpoint(
                "{\"date\":\"2015-02-04T16:10:00+0100\",\"santanderId\":\"1234567890\"}");
    }

    @Test(expected = MongoCheckpointFilterException.class)
    public void updateCheckpointWithMalFormedJson() throws Exception {
        when(context.get("field"))
                .thenReturn("date");
        when(context.get("type"))
                .thenReturn("java.util.Date");
        when(context.get("format"))
                .thenReturn(DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX);
        when(context.get("mongoUri"))
                .thenReturn("mongodb://" + getMongoHost() + "/" + DB_TEST + ".validCollection");
        handler = spy(new MongoCheckpointFilterHandler(new DateCheckpointType(), context));

        handler.updateCheckpoint(
                "{\"date\":,\"santanderId\":\"1234567890\"}");
    }

    private DBObject populateDocument() throws ParseException {
        final DBObject document = new BasicDBObject();
        document.put("santanderID", "documentID");
        document.put("date", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse("2014-12-16T16:32:33"));
        return document;
    }
}