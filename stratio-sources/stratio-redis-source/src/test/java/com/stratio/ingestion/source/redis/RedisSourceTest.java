package com.stratio.ingestion.source.redis;

import static com.stratio.ingestion.source.redis.RedisConstants.DEFAULT_HOST;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import junit.framework.TestCase;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by eruiz on 8/10/15.
 */
@RunWith(JUnit4.class)
public class RedisSourceTest extends TestCase {

    RedisSource source;
    static Channel channel;
    static String host;
    static Integer port;
    JedisPoolConfig poolConfig;

    private Map<String, String> poolProps;

    @Before
    public void setUp() {
        source = new RedisSource();
        channel = new MemoryChannel();

        Configurables.configure(channel, new Context());

        List<Channel> channels = new ArrayList<Channel>();
        channels.add(channel);

        ChannelSelector rcs = new ReplicatingChannelSelector();
        rcs.setChannels(channels);

        source.setChannelProcessor(new ChannelProcessor(rcs));
    }

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
