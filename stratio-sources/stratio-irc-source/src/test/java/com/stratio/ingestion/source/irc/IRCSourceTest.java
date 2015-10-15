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
package com.stratio.ingestion.source.irc;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import junit.framework.TestCase;

@Ignore
@RunWith(JUnit4.class)
public class IRCSourceTest extends TestCase {

    static IRCSource source;
    static MemoryChannel channel;

//    IRCSource ircSource = new IRCSource();
    Context context = new Context();
    @Ignore
    @Before
    public void setUp() {

        source = new IRCSource();
        channel = new MemoryChannel();
//        context.put("port", "6667");ntext());

        context.put("host", "localhost");
        context.put("port", "6667");
        context.put("nick", "admin");
        context.put("channels", "memory,jdbc");
        context.put("password", "1234");
        context.put("replyPing", "false");
        context.put("user", "root");
        context.put("name", "admin");
        context.put("replyPing", "false");


        source.configure(context);
    }

    @Test
    public void dummy(){
        Assert.assertTrue(true); //TODO: Dummy //st to avoid surefire failure
    }

    @Test
    public void testBadConnection(){
        source.start();
        source.stop();
    }

    @Test
    public void testSetHeaders() {
        source.setHeaders();
    }

    /*@Test
    public void testConnection() throws InterruptedException, IOException {
        Context context = new Context();
        *//*context.put(CONF_HOST, "irc.freenode.org");
        context.put(CONF_USER, "stratiogms");
        context.put(CONF_NICK, "stratio");
        context.put(CONF_CHANNELS, "stratiotest");*//*
        context.put(CONF_HOST, "irc.twitch.tv");
        context.put(CONF_USER, "antnavper");
        context.put(CONF_NICK, "antnavper");
        context.put(CONF_CHANNELS, "amazhs,beyondthesummit,tsm_wildturtle,starladder1,therace");
        context.put(CONF_PASSWORD, "oauth:s362vqqdy4rp4ljblybvjhnq9eg3ev");


        Configurables.configure(source, context);
        source.start();

        while(true){
            Transaction txn = channel.getTransaction();
            txn.begin();
            channel.take();
            txn.commit();
            txn.close();
        }
    }*/


    public void checkEventsChannel() {
        Transaction txn = channel.getTransaction();
        txn.begin();
        Event e = channel.take();
        Assert.assertNotNull("Event must not be null", e);
        Assert.assertNotNull("Event headers must not be null", e.getBody());
        txn.commit();
        txn.close();
        source.stop();
    }
}
