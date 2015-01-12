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

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flume.Context;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.stratio.ingestion.source.batch.rest.checkpoint.DateCheckpointType;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.ArtifactStoreBuilder;
import de.flapdoodle.embed.mongo.config.DownloadConfigBuilder;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongoCmdOptionsBuilder;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.io.file.Files;
import de.flapdoodle.embed.process.runtime.Network;

@RunWith(MockitoJUnitRunner.class)
public class MongoCheckpointHandlerTest {

    public static final String DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX = "yyyy-MM-dd'T'HH:mm:ssXXX";
    private MongodProcess mongod;
    private MongoCheckpointHandler retrieval;

    @Mock
    private Context context;

    public static MongodExecutable mongodExecutable = null;
    public static final Integer PORT = 27890;
    public final static String DB_FOLDER_NAME = System.getProperty("user.home") +
            File.separator + "mongoEntityRDDTest";
    private MongoClient mongoClient;

    @BeforeClass
    public static void init() throws IOException {
        Command command = Command.MongoD;

        try {
            Files.forceDelete(new File(DB_FOLDER_NAME));
        } catch (Exception e) {

        }

        new File(DB_FOLDER_NAME).mkdirs();

        IMongodConfig mongodConfig = new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .configServer(false)
                .replication(new Storage(DB_FOLDER_NAME, null, 0))
                .net(new Net(PORT, Network.localhostIsIPv6()))
                .cmdOptions(new MongoCmdOptionsBuilder()
                        .syncDelay(10)
                        .useNoPrealloc(true)
                        .useSmallFiles(true)
                        .useNoJournal(true)
                        .build())
                .build();

        IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
                .defaults(command)
                .artifactStore(new ArtifactStoreBuilder()
                        .defaults(command)
                        .download(new DownloadConfigBuilder()
                                .defaultsForCommand(command)
                                .downloadPath("https://s3-eu-west-1.amazonaws.com/stratio-mongodb-distribution/")))
                .build();

        MongodStarter runtime = MongodStarter.getInstance(runtimeConfig);

        mongodExecutable = null;

        mongodExecutable = runtime.prepare(mongodConfig);

        mongodExecutable.start();
    }

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);

    }

    @Test(expected = NullPointerException.class)
    public void getLastCheckpointWithNoValidCheckpointField() throws Exception {
        when(context.getString(MongoCheckpointHandler.MONGO_URI))
                .thenReturn("mongodb://127.0.0.1:27890/mydb.profiles");

        retrieval = new MongoCheckpointHandler(context, new DateCheckpointType());
    }

    @Test(expected = NullPointerException.class)
    public void getLastCheckpointWithNoValidCheckpointType() throws Exception {
        when(context.getString(MongoCheckpointHandler.CONF_CHECKPOINT_FIELD))
                .thenReturn("validName");

        retrieval = new MongoCheckpointHandler(context, new DateCheckpointType());
    }

    @Test
    public void getLastCheckpointWithNoExistingDB() throws Exception {
        when(context.getString(MongoCheckpointHandler.CONF_CHECKPOINT_FIELD))
                .thenReturn("validName");
        when(context.getString(MongoCheckpointHandler.CONF_CHECKPOINT_TYPE))
                .thenReturn("java.util.Date");
        when(context.getString("datePattern", DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX))
                .thenReturn(DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX);
        when(context.getString(MongoCheckpointHandler.MONGO_URI))
                .thenReturn("mongodb://127.0.0.1:27890/noExistingDB.emptyCollection");
        retrieval = new MongoCheckpointHandler(context, new DateCheckpointType());
        mongoClient = new MongoClient("localhost", PORT);
        mongoClient.getDB("test").createCollection("emptyCollection", null);

        retrieval = new MongoCheckpointHandler(context, new DateCheckpointType());
        final String lastCheckpoint = retrieval.getLastCheckpoint(context);
        assertThat(lastCheckpoint).isNotNull();
        assertThat(lastCheckpoint)
                .isEqualTo(new SimpleDateFormat(DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX).format(new Date(0)));

    }

    @Test
    public void getLastCheckPointWithEmptyCollection() throws UnknownHostException {
        when(context.getString(MongoCheckpointHandler.CONF_CHECKPOINT_FIELD))
                .thenReturn("validName");
        when(context.getString(MongoCheckpointHandler.CONF_CHECKPOINT_TYPE))
                .thenReturn("java.util.Date");
        when(context.getString("datePattern", DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX))
                .thenReturn(DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX);
        when(context.getString(MongoCheckpointHandler.MONGO_URI))
                .thenReturn("mongodb://127.0.0.1:27890/test.emptyCollection");
        retrieval = new MongoCheckpointHandler(context, new DateCheckpointType());
        mongoClient = new MongoClient("localhost", PORT);
        mongoClient.getDB("test").createCollection("emptyCollection", null);

        final String lastCheckpoint = retrieval.getLastCheckpoint(context);
        assertThat(lastCheckpoint).isNotNull();
        assertThat(lastCheckpoint)
                .isEqualTo(new SimpleDateFormat(DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX).format(new Date(0)));

    }

    @Test
    public void getLastCheckPointWithValidCollection() throws UnknownHostException, ParseException {
        when(context.getString(MongoCheckpointHandler.CONF_CHECKPOINT_FIELD))
                .thenReturn("date");
        when(context.getString(MongoCheckpointHandler.CONF_CHECKPOINT_TYPE))
                .thenReturn("java.util.Date");
        when(context.getString(MongoCheckpointHandler.MONGO_URI))
                .thenReturn("mongodb://127.0.0.1:27890/test.validCollection");
        when(context.getString("datePattern", DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX))
                .thenReturn(DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX);
        retrieval = new MongoCheckpointHandler(context, new DateCheckpointType());
        mongoClient = new MongoClient("localhost", PORT);
        mongoClient.getDB("test").createCollection("validCollection", null);
        mongoClient.getDB("test").getCollection("validCollection").save(populateDocument());

        final String lastCheckpoint = retrieval.getLastCheckpoint(context);
        assertThat(lastCheckpoint).isNotNull();
        assertThat(lastCheckpoint).isEqualTo("2014-12-16T16:32:33+01:00");

    }

    private DBObject populateDocument() throws ParseException {
        final DBObject document = new BasicDBObject();
        document.put("santanderID", "documentID");
        document.put("date", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse("2014-12-16T16:32:33"));
        return document;
    }
}