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
package com.stratio.ingestion.sink.jdbc;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.fest.assertions.Assertions.*;

@RunWith(JUnit4.class)
public class JDBCSinkTest {

    @Test
    public void mappedWithH2() throws Exception {
        Class.forName("org.h2.Driver").newInstance();
        Connection conn = DriverManager.getConnection("jdbc:h2:/tmp/jdbcsink_test");
        conn.prepareStatement("DROP TABLE public.test IF EXISTS;").execute();
        conn.prepareStatement("CREATE TABLE public.test (myInteger INTEGER, myString VARCHAR, myId BIGINT AUTO_INCREMENT PRIMARY KEY);").execute();
        conn.commit();
        conn.close();

        Context ctx = new Context();
        ctx.put("driver", "org.h2.Driver");
        ctx.put("connectionString", "jdbc:h2:/tmp/jdbcsink_test");
        ctx.put("table", "test");
        ctx.put("sqlDialect", "H2");
        ctx.put("batchSize", "1");

        JDBCSink jdbcSink = new JDBCSink();

        Configurables.configure(jdbcSink, ctx);

        Context channelContext = new Context();
        channelContext.put("capacity", "10000");
        channelContext.put("transactionCapacity", "200");

        Channel channel = new MemoryChannel();
        channel.setName("junitChannel");
        Configurables.configure(channel, channelContext);

        jdbcSink.setChannel(channel);

        channel.start();
        jdbcSink.start();

        Transaction tx = channel.getTransaction();
        tx.begin();

        Map<String, String> headers = new HashMap<String, String>();
        headers.put("myString", "bar"); // Overwrites the value defined in JSON body
        headers.put("myInteger", "64");
        headers.put("myBoolean", "true");
        headers.put("myDouble", "1.0");
        headers.put("myNull", "foobar");

        Date myDate = new Date();
        headers.put("myDate", Long.toString(myDate.getTime()));

        headers.put("myString2", "baz");

        Event event = EventBuilder.withBody(new byte[0], headers);
        channel.put(event);

        tx.commit();
        tx.close();

        jdbcSink.process();

        jdbcSink.stop();
        channel.stop();

        conn = DriverManager.getConnection("jdbc:h2:/tmp/jdbcsink_test");
        ResultSet resultSet = conn.prepareStatement("SELECT * FROM public.test").executeQuery();
        resultSet.next();
        assertThat(resultSet.getInt("myInteger")).isEqualTo(64);
        assertThat(resultSet.getString("myString")).isEqualTo("bar");
        conn.close();
    }

    @Test
    public void mappedWithDerby() throws Exception {
        FileUtils.deleteDirectory(new File("test_derby_db"));
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
        Connection conn = DriverManager.getConnection("jdbc:derby:test_derby_db;create=true");
        conn.prepareStatement("CREATE TABLE TEST (myInteger INT, myString VARCHAR(255), myId INT NOT NULL GENERATED ALWAYS AS IDENTITY \n" +
                "\t(START WITH 1, INCREMENT BY 1), PRIMARY KEY(myId))").execute();
        conn.commit();
        conn.close();

        Context ctx = new Context();
        ctx.put("driver", "org.apache.derby.jdbc.EmbeddedDriver");
        ctx.put("connectionString", "jdbc:derby:test_derby_db");
        ctx.put("table", "test");
        ctx.put("sqlDialect", "DERBY");
        ctx.put("batchSize", "1");

        JDBCSink jdbcSink = new JDBCSink();

        Configurables.configure(jdbcSink, ctx);

        Context channelContext = new Context();
        channelContext.put("capacity", "10000");
        channelContext.put("transactionCapacity", "200");

        Channel channel = new MemoryChannel();
        channel.setName("junitChannel");
        Configurables.configure(channel, channelContext);

        jdbcSink.setChannel(channel);

        channel.start();
        jdbcSink.start();

        Transaction tx = channel.getTransaction();
        tx.begin();

        Map<String, String> headers = new HashMap<String, String>();
        headers.put("myString", "bar"); // Overwrites the value defined in JSON body
        headers.put("myInteger", "64");
        headers.put("myBoolean", "true");
        headers.put("myDouble", "1.0");
        headers.put("myNull", "foobar");

        Date myDate = new Date();
        headers.put("myDate", Long.toString(myDate.getTime()));

        headers.put("myString2", "baz");

        Event event = EventBuilder.withBody(new byte[0], headers);
        channel.put(event);

        tx.commit();
        tx.close();

        jdbcSink.process();

        jdbcSink.stop();
        channel.stop();

        conn = DriverManager.getConnection("jdbc:derby:test_derby_db");
        ResultSet resultSet = conn.prepareStatement("SELECT * FROM TEST").executeQuery();
        resultSet.next();
        assertThat(resultSet.getInt("myInteger")).isEqualTo(64);
        assertThat(resultSet.getString("myString")).isEqualTo("bar");
        conn.close();

        try {
            DriverManager.getConnection("jdbc:derby:;shutdown=true");
        } catch (SQLException ex) {

        }
    }

    @Test
    public void templateWithH2() throws Exception {
        Class.forName("org.h2.Driver").newInstance();
        Connection conn = DriverManager.getConnection("jdbc:h2:/tmp/jdbcsink_test");
        conn.prepareStatement("DROP TABLE public.test IF EXISTS;").execute();
        conn.prepareStatement("CREATE TABLE public.test (myInteger INTEGER, myString VARCHAR, myId BIGINT AUTO_INCREMENT PRIMARY KEY);").execute();
        conn.commit();
        conn.close();

        Context ctx = new Context();
        ctx.put("driver", "org.h2.Driver");
        ctx.put("connectionString", "jdbc:h2:/tmp/jdbcsink_test");
        ctx.put("sqlDialect", "H2");
        ctx.put("batchSize", "1");
        ctx.put("sql", "INSERT INTO \"PUBLIC\".\"TEST\" (\"MYINTEGER\", \"MYSTRING\") VALUES (cast(${header.myInteger:int} as integer), cast(${header.myString:varchar} as varchar))");

        JDBCSink jdbcSink = new JDBCSink();

        Configurables.configure(jdbcSink, ctx);

        Context channelContext = new Context();
        channelContext.put("capacity", "10000");
        channelContext.put("transactionCapacity", "200");

        Channel channel = new MemoryChannel();
        channel.setName("junitChannel");
        Configurables.configure(channel, channelContext);

        jdbcSink.setChannel(channel);

        channel.start();
        jdbcSink.start();

        Transaction tx = channel.getTransaction();
        tx.begin();

        Map<String, String> headers = new HashMap<String, String>();
        headers.put("myString", "bar"); // Overwrites the value defined in JSON body
        headers.put("myInteger", "64");
        headers.put("myBoolean", "true");
        headers.put("myDouble", "1.0");
        headers.put("myNull", "foobar");

        Date myDate = new Date();
        headers.put("myDate", Long.toString(myDate.getTime()));

        headers.put("myString2", "baz");

        Event event = EventBuilder.withBody(new byte[0], headers);
        channel.put(event);

        tx.commit();
        tx.close();

        jdbcSink.process();

        jdbcSink.stop();
        channel.stop();

        conn = DriverManager.getConnection("jdbc:h2:/tmp/jdbcsink_test");

        ResultSet rs = conn.prepareStatement("SELECT count(*) AS count FROM public.test").executeQuery();
        rs.next();
        assertThat(rs.getInt("count")).isEqualTo(1);

        rs = conn.prepareStatement("SELECT * FROM public.test").executeQuery();
        rs.next();
//        for (int i = 1; i <= 3; i++) {
//            System.out.println(rs.getString(i));
//        }
        assertThat(rs.getInt("myInteger")).isEqualTo(64);
        assertThat(rs.getString("myString")).isEqualTo("bar");
        conn.close();

    }

}
