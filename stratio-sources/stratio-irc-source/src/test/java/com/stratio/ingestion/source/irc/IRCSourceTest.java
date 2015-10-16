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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import junit.framework.TestCase;

@RunWith(JUnit4.class)
public class IRCSourceTest extends TestCase {

    static IRCSource source;
    static MemoryChannel channel;

    Context context = new Context();

    @Before
    public void setUp() {

        source = new IRCSource();
        channel = new MemoryChannel();

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

    @Test(expected = NullPointerException.class)
    public void testBadConnection(){
        try {
            source.start();
            source.stop();
        }catch(Exception e){
            throw new NullPointerException();
        }
    }

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
