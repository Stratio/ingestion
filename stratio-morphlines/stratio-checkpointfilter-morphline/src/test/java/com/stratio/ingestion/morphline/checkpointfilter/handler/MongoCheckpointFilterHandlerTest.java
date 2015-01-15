package com.stratio.ingestion.morphline.checkpointfilter.handler;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.stratio.ingestion.morphline.checkpointfilter.type.DateCheckpointType;

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

public class MongoCheckpointFilterHandlerTest {
    public static final String DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SS_XXX = "yyyy-MM-dd'T'HH:mm:ssXXX";
    private MongodProcess mongod;
    private MongoCheckpointFilterHandler handler;

    @Mock
    private Map<String, String> context;

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
        when(context.get("mongoUri"))
                .thenReturn("mongodb://127.0.0.1:27890/mydb.profiles");

        handler = new MongoCheckpointFilterHandler(new DateCheckpointType(), context);
    }

    @Test(expected = NullPointerException.class)
    public void getLastCheckpointWithNoValidCheckpointType() throws Exception {
        when(context.get("field"))
                .thenReturn("validName");

        handler = new MongoCheckpointFilterHandler(new DateCheckpointType(), context);
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
                .thenReturn("mongodb://127.0.0.1:27890/noExistingDB.emptyCollection");
        handler = new MongoCheckpointFilterHandler(new DateCheckpointType(), context);
        mongoClient = new MongoClient("localhost", PORT);
        mongoClient.getDB("test").createCollection("emptyCollection", null);

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
                .thenReturn("mongodb://127.0.0.1:27890/test.emptyCollection");
        handler = new MongoCheckpointFilterHandler(new DateCheckpointType(), context);
        mongoClient = new MongoClient("localhost", PORT);
        mongoClient.getDB("test").createCollection("emptyCollection", null);

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
                .thenReturn("mongodb://127.0.0.1:27890/test.validCollection");
        handler = new MongoCheckpointFilterHandler(new DateCheckpointType(), context);
        mongoClient = new MongoClient("localhost", PORT);
        mongoClient.getDB("test").createCollection("validCollection", null);
        mongoClient.getDB("test").getCollection("validCollection").save(populateDocument());

        final String lastCheckpoint = handler.getLastCheckpoint(context);
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