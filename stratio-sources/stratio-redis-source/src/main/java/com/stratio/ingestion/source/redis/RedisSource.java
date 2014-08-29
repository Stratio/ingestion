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

import com.google.common.collect.Maps;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.nio.charset.Charset;
import java.util.Map;

/**
 *
 * Redis Subscribe Source
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
 * </ul>
 * </p>
 *
 */
public class RedisSource extends AbstractSource implements Configurable, EventDrivenSource {
	
	private static final Logger log = LoggerFactory.getLogger(RedisSource.class);

    private ChannelProcessor channelProcessor;
	
	private Jedis jedis;
    private String host;
    private Integer port;
    private String charset;
    private String [] channels;
    private String [] patterns;

    boolean pattern = false;

	@Override
	public void configure(Context context) {

        host = context.getString(RedisConstants.CONF_HOST, RedisConstants.DEFAULT_HOST);
        port = context.getInteger(RedisConstants.CONF_PORT, RedisConstants.DEFAULT_PORT);
        charset = context.getString(RedisConstants.CONF_CHARSET, RedisConstants.DEFAULT_CHARSET);
        String rawChannels = context.getString(RedisConstants.CONF_CHANNELS);
        String rawPatterns = context.getString(RedisConstants.CONF_PCHANNELS);
        if(null != rawChannels){
            channels = rawChannels.trim().split(",");
            pattern = false;
        } else if (null != rawPatterns){
            patterns = rawPatterns.trim().split(",");
            pattern = true;
        } else {
            throw new RuntimeException("You must set " + RedisConstants.CONF_CHANNELS  + " or " + RedisConstants.CONF_PCHANNELS + " property.");
        }

        log.info("Redis Source Configured");
	}
	
	@Override
	public synchronized void start() {
        super.start();

        channelProcessor = getChannelProcessor();

        jedis = new Jedis(host, port);
        log.info("Redis Connected. (host: " + host + ", port: " + String.valueOf(port) + ")");

        new Thread(new SubscribeManager()).start();
	}
	
	@Override
	public synchronized void stop() {
        super.stop();
        jedis.close();
	}

    private class SubscribeManager implements Runnable {

        @Override
        public void run() {
            log.info("Subscribe Manager Thread is started.");

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

            if(pattern){
                for(String pattern : patterns){
                    log.info("Jedis is going to subscribe to pattern: " + pattern);
                }

                jedis.psubscribe(jedisPubSub, patterns);
            } else {
                for(String channel : channels){
                    log.info("Jedis is going to subscribe to channel: " + channel);
                }

                jedis.subscribe(jedisPubSub, channels);
            }
        }
    }
	
}
