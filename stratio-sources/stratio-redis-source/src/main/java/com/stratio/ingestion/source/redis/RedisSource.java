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

import static com.stratio.ingestion.source.redis.RedisConstants.*;

import java.nio.charset.Charset;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 *
 * Redis Subscribe Source with connection Pool.
 *
 * Configuration parameters are:
 *
 * <p>
 * <ul>
 * <li><tt>host</tt> <em>(string)</em>: Redist host. Default: localhost.</li>
 * <li><tt>port</tt> <em>(integer)</em>: Redis port. Default: 6379.</li>
 * <li><tt>subscribe</tt> <em>(string)</em>: Channels to subscribe. Comma separated channels values.</li>
 * <li><tt>psubscribe</tt> <em>(string)</em>: Channels to subscribe with given pattern./li>
 * <li><tt></tt>charset</tt> <em>(string)</em>: Charset. Default: utf-8. </li>
 * <li><tt>pool.<property></tt></li>: Prefix for pool properties. Set whatever property you want to Jedis Pool.
 * </ul>
 * </p>
 *
 */
public class RedisSource extends AbstractSource implements Configurable, EventDrivenSource {
	
	private static final Logger log = LoggerFactory.getLogger(RedisSource.class);

    private ChannelProcessor channelProcessor;
	
    private JedisPool jedisPool;
    private String host;
    private Integer port;
    private String charset;
    private String [] channels;
    private String [] patterns;
    private Map<String, String> poolProps;

    boolean pattern = false;

	@Override
	public void configure(Context context) {

        host = context.getString(RedisConstants.CONF_HOST, DEFAULT_HOST);
        port = context.getInteger(RedisConstants.CONF_PORT, DEFAULT_PORT);
        charset = context.getString(RedisConstants.CONF_CHARSET, DEFAULT_CHARSET);
        String rawChannels = context.getString(CONF_CHANNELS);
        String rawPatterns = context.getString(CONF_PCHANNELS);
        if(null != rawChannels){
            channels = rawChannels.trim().split(",");
            pattern = false;
        } else if (null != rawPatterns){
            patterns = rawPatterns.trim().split(",");
            pattern = true;
        } else {
            throw new RuntimeException("You must set " + CONF_CHANNELS  + " or " + CONF_PCHANNELS + " property.");
        }

        poolProps = context.getSubProperties("pool.");

        log.info("Redis Source Configured");
	}
	
	@Override
	public synchronized void start() {
        super.start();

        channelProcessor = getChannelProcessor();

        init();
        log.info("Redis Connected. (host: " + host + ", port: " + String.valueOf(port) + ")");

        new SubscribeManager().run();
	}
	
	@Override
	public synchronized void stop() {
        super.stop();
        jedisPool.destroy();
	}

    private class SubscribeManager implements Runnable {

        Jedis subscriber;

        @Override
        public void run() {
            log.info("Subscribe Manager Thread is started.");
            subscriber = jedisPool.getResource();

            JedisPubSub jedisPubSub = new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    Event event = EventBuilder.withBody(message, Charset.forName(charset));
                    channelProcessor.processEvent(event);
                }

                @Override
                public void onPMessage(String pattern, String channel, String message) {
                    Map<String, String> headers = Maps.newHashMap();
                    headers.put("channel", channel);
                    Event event = EventBuilder.withBody(message, Charset.forName(charset), headers);
                    channelProcessor.processEvent(event);
                }

                @Override
                public void onSubscribe(String channel, int subscribedChannels) {
                    log.info("onSubscribe (Channel: " + channel + ")");
                }

                @Override
                public void onUnsubscribe(String channel, int subscribedChannels) {
                    log.info("onUnsubscribe (Channel: " + channel + ")");
                }

                @Override
                public void onPUnsubscribe(String Pattern, int subscribedChannels) {
                    log.info("onPUnSubscribe (Pattern: " + Pattern + ")");
                }

                @Override
                public void onPSubscribe(String pattern, int subscribedChannels) {
                    log.info("onPSubscribe (Pattern: " + pattern + ")");
                }
            };

            while (true) {
                if (pattern) {
                    for (String pattern : patterns) {
                        log.info("Jedis is going to subscribe to pattern: " + pattern);
                    }
                    try {
                        subscriber.psubscribe(jedisPubSub, patterns);
                    } catch (JedisConnectionException ex) {
                        jedisPool.returnBrokenResource(subscriber);
                        subscriber = jedisPool.getResource();
                    }
                } else {
                    for (String channel : channels) {
                        log.info("Jedis is going to subscribe to channel: " + channel);
                    }
                    try {
                        subscriber.subscribe(jedisPubSub, channels);
                    } catch (JedisConnectionException ex) {
                        jedisPool.returnBrokenResource(subscriber);
                        subscriber = jedisPool.getResource();
                        log.info("Active connections: " + jedisPool.getNumActive());
                    }
                }
            }
        }
    }

    private void init() {
        try {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            String prop;

            if((prop = poolProps.get(CONF_TESTONBORROW)).equals(null)){
                poolConfig.setTestOnBorrow(DEFAULT_TESTONBORROW);
            } else {
                log.info("Setting testOnBorrow property to " + prop);
                poolConfig.setTestOnBorrow(Boolean.valueOf(prop));
            }
            if((prop = poolProps.get(CONF_MAXTOTAL)).equals(null)){
                poolConfig.setMaxTotal(DEFAULT_MAXTOTAL);
            } else {
                log.info("Setting maxTotal property to " + prop);
                poolConfig.setMaxTotal(Integer.valueOf(prop));
            }
            if((prop = poolProps.get(CONF_MAXIDLE)).equals(null)){
                poolConfig.setMaxTotal(DEFAULT_MAXIDLE);
            } else {
                log.info("Setting maxIdle property to " + prop);
                poolConfig.setMaxIdle(Integer.valueOf(prop));
            }
            if((prop = poolProps.get(CONF_MINIDLE)).equals(null)){
                poolConfig.setMinIdle(DEFAULT_MINIDLE);
            } else {
                log.info("Setting minIdle property to " + prop);
                poolConfig.setMinIdle(Integer.valueOf(prop));
            }
            if((prop = poolProps.get(CONF_MAXWAITINMILLIS)).equals(null)){
                poolConfig.setMaxWaitMillis(DEFAULT_MAXWAITINMILLIS);
            } else {
                log.info("Setting maxWaitInMillis property to " + prop);
                poolConfig.setMaxWaitMillis(Integer.valueOf(prop));
            }
            if((prop = poolProps.get(CONF_TESTWHILEIDLE)).equals(null)){
                poolConfig.setTestWhileIdle(DEFAULT_TESTWHILEIDLE);
            } else {
                log.info("Setting testWhileIdle property to " + prop);
                poolConfig.setTestWhileIdle(Boolean.valueOf(prop));
            }
            if((prop = poolProps.get(CONF_TESTONRETURN)).equals(null)){
                poolConfig.setTestOnReturn(DEFAULT_TESTONRETURN);
            } else {
                log.info("Setting testOnReturn property to " + prop);
                poolConfig.setTestOnReturn(Boolean.valueOf(prop));
            }
            if((prop = poolProps.get(CONF_MINEVICTABLEIDLETIMEINMILLIS)).equals(null)){
                poolConfig.setMinEvictableIdleTimeMillis(DEFAULT_MINEVICTABLEIDLETIMEINMILLIS);
            } else {
                log.info("Setting minEvictableIdleTimeInMillis property to " + prop);
                poolConfig.setMinEvictableIdleTimeMillis(Integer.valueOf(prop));
            }
            if((prop = poolProps.get(CONF_TIMEBETWEETNEVICTIONRUNSMILLIS)).equals(null)){
                poolConfig.setTimeBetweenEvictionRunsMillis(DEFAULT_TIMEBETWEETNEVICTIONRUNSMILLIS);
            } else {
                log.info("Setting timeBetweenEvictionRunMillis property to " + prop);
                poolConfig.setTimeBetweenEvictionRunsMillis(Integer.valueOf(prop));
            }
            if((prop = poolProps.get(CONF_NUMTESTSPEREVICTIONRUN)).equals(null)){
                poolConfig.setNumTestsPerEvictionRun(DEFAULT_NUMTESTSPEREVICTIONRUN);
            } else {
                log.info("Setting numTestsPerEvictionRun property to " + prop);
                poolConfig.setNumTestsPerEvictionRun(Integer.valueOf(prop));
            }

            // create JEDIS pool
            this.jedisPool = new JedisPool(poolConfig, host, port);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
