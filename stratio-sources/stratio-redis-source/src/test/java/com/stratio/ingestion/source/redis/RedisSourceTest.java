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
package com.stratio.ingestion.source.redis;

import static com.stratio.ingestion.source.redis.RedisConstants.DEFAULT_HOST;

import java.io.IOException;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import junit.framework.TestCase;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by eruiz on 13/10/15.
 */
@RunWith(JUnit4.class)
public class RedisSourceTest extends TestCase {

    RedisSource source;
    static Channel channel;
    static String host;
    static Integer port;
    JedisPoolConfig poolConfig;

    private Map<String, String> poolProps;

//    @Before
//    public void setUp() {
//        source = new RedisSource();
//        channel = new MemoryChannel();
//
//        Configurables.configure(channel, new Context());
//
//        List<Channel> channels = new ArrayList<Channel>();
//        channels.add(channel);
//
//        ChannelSelector rcs = new ReplicatingChannelSelector();
//        rcs.setChannels(channels);
//
//        source.setChannelProcessor(new ChannelProcessor(rcs));
//    }

    @Ignore
    @Test
    //            (expected = redis.clients.jedis.exceptions.JedisConnectionException.class)
    public void testConnection() throws InterruptedException, IOException {

        Context context = new Context();
        context.put(RedisConstants.CONF_HOST, DEFAULT_HOST);
        context.put(RedisConstants.CONF_CHANNELS, "channel1");
        context.put("pool.testOnBorrow", "true");
        context.put("pool.maxTotal", "100");
        context.put("pool.maxIdle", "10");
        context.put("pool.minIdle", "2");
        context.put("pool.maxWaitInMillis", "100");
        context.put("pool.testWhileIdle", "true");
        context.put("pool.testOnReturn", "true");
        context.put("pool.minEvictableIdleTimeInMillis", "10000");
        context.put("pool.timeBetweenEvictionRunsMillis", "5000");
        context.put("pool.numTestsPerEvictionRun", "10");

        source.configure(context);

        Assert.assertNull(source.getName());
        Assert.assertNotNull(source);

    }

}

