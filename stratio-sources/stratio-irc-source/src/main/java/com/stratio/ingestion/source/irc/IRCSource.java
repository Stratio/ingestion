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

import static com.stratio.ingestion.source.irc.IRCConstants.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.schwering.irc.lib.IRCConnection;
import org.schwering.irc.lib.IRCEventListener;
import org.schwering.irc.lib.IRCModeParser;
import org.schwering.irc.lib.IRCUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

/**
 *
 * Connect IRC channels and retrieve messages from them.
 *
 * Configuration parameters are:
 *
 * <p>
 * <ul>
 * <li><tt>address</tt> <em>(string, required)</em>: Address to listen snmp traps. Default: localhost</li>
 * <li><tt>snmpTrapPort</tt> <em>(integer, required)</em>: Port to listen snmp traps. Default: 162</li>
 * <li><tt>snmpVersion</tt> <em>(string)</em>: SNMP Protocol version. Possible values: V1,V2c,V3. Default: V1</li>
 * <li><tt>snmpTrapVersion</tt> <em>(string)</em>: SNMP Trap Protocol version. Possible values: V1,V2c,V3. Default: V1</li>
 * <li><tt>encryptionType</tt> <em>(string)</em>: Encryption. Possible values: SHA, MD5. Default: MD5</li>
 * <li><tt>authenticationType</tt> <em>(string)</em>: SNMP Authentication. Possible values: AUTH_NOPRIV, NOAUTH_NOPRIV, AUTH_PRIV. Default: NOAUTH_NOPRIV</li>
 * <li><tt>username</tt> <em>(string)</em>: username. Required when authenticationType -> AUTH_NOPRIV, AUTH_PRIV</li>
 * <li><<tt>password</tt> <em>(boolean)</em>: password. Required when authenticationType -> AUTH_NOPRIV, AUTH_PRIV</li>
 *  <li><tt>privacyProtocol</tt> <em>(string)</em>: Privacy protocol. Required when authenticationType -> AUTH_PRIV. Possible values: PrivDES, Priv3DES, PrivAES128, PrivAES192, PrivAES256, PrivAES192With3DESKeyExtension, PrivAES256With3DESKeyExtension. Default: PRIVDES.</li>
 * <li><<tt>privacyPassphrase</tt> <em>(boolean)</em>: Privacy passphrase. Required when authenticationType -> AUTH_PRIV.</li>
 * </ul>
 * </p>
 *
 */
public class IRCSource extends AbstractSource implements Configurable, EventDrivenSource {

    private static final Logger logger = LoggerFactory.getLogger(IRCSource.class);

    private IRCConnection connection = null;

    private String hostname;
    private Integer port;
    private String nick;
    private String password;
    private String user;
    private String name;
    private Boolean replyPing;
    private List<String> channels;
    private SourceCounter sourceCounter;

    class IRCConnectionListener implements IRCEventListener {

        Map<String, String> headers = new HashMap<String, String>();

        public void onRegistered() {
            logger.info("IRC on Registered called");
            headers.put(IRCConstants.HEADER_TYPE, "registered");
            send("", headers);
        }

        public void onDisconnected() {
            logger.error("IRC source disconnected");
            headers.put(IRCConstants.HEADER_TYPE, "disconnected");
            send("", headers);
        }

        public void onError(String msg) {
            logger.error("IRC source error: {}", msg);
            headers.put(IRCConstants.HEADER_TYPE, "error");
            send(msg, headers);
        }

        public void onError(int num, String msg) {
            logger.debug("IRC source error: {} - {}", num, msg);
            headers.put(IRCConstants.HEADER_TYPE, "error");
            headers.put(IRCConstants.HEADER_IDENTIFIER, String.valueOf(num));
            send(msg, headers);
        }

        public void onInvite(String chan, IRCUser u, String nickPass) {
            logger.debug("User {} was invited to channel {}.", u.getNick(), chan);
            headers.put(IRCConstants.HEADER_TYPE, "invite");
            headers.put(IRCConstants.HEADER_CHANNEL, chan);
            headers.putAll(getUser(u));
            send("", headers);
        }

        public void onJoin(String chan, IRCUser u) {
            logger.debug("User {} joined to channel {}.", u.getNick(), chan);
            headers.put(IRCConstants.HEADER_TYPE, "join");
            headers.put(IRCConstants.HEADER_CHANNEL, chan);
            headers.putAll(getUser(u));
            send("", headers);
        }

        public void onKick(String chan, IRCUser u, String nickPass, String msg) {
            logger.debug("User {} was kicked from channel {}.", u.getNick(), chan);
            headers.put(IRCConstants.HEADER_TYPE, "kick");
            headers.put(IRCConstants.HEADER_CHANNEL, chan);
            headers.putAll(getUser(u));
            headers.put(IRCConstants.HEADER_NICKPASS, nickPass);
            send(msg, headers);
        }

        public void onMode(IRCUser u, String nickPass, String mode) {
            logger.debug("User {} changed mode.", u.getNick());
            headers.put(IRCConstants.HEADER_TYPE, "mode");
            headers.putAll(getUser(u));
            headers.put(IRCConstants.HEADER_NICKPASS, nickPass);
            headers.put(IRCConstants.HEADER_MODE, mode);
            send("", headers);
        }

        public void onMode(String chan, IRCUser u, IRCModeParser mp) {
            logger.debug("User {} changed {} channel mode.", u.getNick(), chan);
            headers.put(IRCConstants.HEADER_TYPE, "mode");
            headers.put(IRCConstants.HEADER_CHANNEL, chan);
            headers.putAll(getUser(u));
            headers.put(IRCConstants.HEADER_IRCMODEPARSER, mp.toString());
            send("", headers);
        }

        public void onNick(IRCUser u, String nickNew) {
            logger.debug("User {} changed his nick to {} channel mode.", u.getNick(), nickNew);
            headers.put(IRCConstants.HEADER_TYPE, "nick");
            headers.putAll(getUser(u));
            headers.put(IRCConstants.HEADER_NEWNICK, u.getNick());
            send("", headers);
        }

        public void onNotice(String target, IRCUser u, String msg) {
            logger.debug("User {}  noticed {} to {}.", u.getNick(), msg, target);
            headers.put(IRCConstants.HEADER_TYPE, "notice");
            headers.putAll(getUser(u));
            headers.put(IRCConstants.HEADER_TARGET, target);
            send(msg, headers);
        }

        public void onPart(String chan, IRCUser u, String msg) {
            logger.debug("User {} is part of channel {}.", u.getNick(), chan);
            headers.put(IRCConstants.HEADER_TYPE, "part");
            headers.put(IRCConstants.HEADER_CHANNEL, chan);
            headers.putAll(getUser(u));
            send(msg, headers);
        }

        public void onPrivmsg(String chan, IRCUser u, String msg) {
            logger.debug("User {} wrote a message to {} - {}", u.getNick(), chan, msg);
            headers.put(IRCConstants.HEADER_TYPE, "privmsg");
            headers.put(IRCConstants.HEADER_CHANNEL, chan);
            headers.putAll(getUser(u));
            send(msg, headers);
        }

        public void onQuit(IRCUser u, String msg) {
            logger.debug("User {} left the chat.");
            headers.put(IRCConstants.HEADER_TYPE, "quit");
            headers.putAll(getUser(u));
            send(msg, headers);
        }

        public void onReply(int num, String value, String msg) {
            logger.debug("A numeric reply with identifier {} and value {} was received.", num, value);
            headers.put(IRCConstants.HEADER_TYPE, "reply");
            send(msg, headers);
        }

        public void onTopic(String chan, IRCUser u, String topic) {
            logger.debug("User {} changed {} channel topic to {}", u.getNick(), chan, topic);
            headers.put(IRCConstants.HEADER_TYPE, "topic");
            headers.putAll(getUser(u));
            headers.put(IRCConstants.HEADER_CHANNEL, chan);
            headers.put(IRCConstants.HEADER_TOPIC, topic);
            send(topic, headers);
        }

        public void onPing(String p) {
            logger.debug("Ping {}." + p);
            headers.put(IRCConstants.HEADER_TYPE, "ping");
            headers.put(IRCConstants.HEADER_TYPE, "unknown");
            send(p, headers);

            if(replyPing){
                connection.doPong(p);
            }
        }

        public void unknown(String prefix, String command, String middle, String trailing) {
            logger.debug("Unknown event was received. ");
            headers.put(IRCConstants.HEADER_TYPE, "unknown");
            headers.put(IRCConstants.HEADER_TRAILING, trailing);
            headers.put(IRCConstants.HEADER_PREFIX, middle);
            headers.put(IRCConstants.HEADER_COMMAND, command);
            headers.put(IRCConstants.HEADER_MIDDLE, middle);
            send("", headers);
        }



        private Map<String, String> getUser(IRCUser u) {
            Map<String, String> usermap = new HashMap();
            usermap.put(IRCConstants.HEADER_USERNAME, u.getUsername());
            usermap.put(IRCConstants.HEADER_SERVERNAME, u.getServername());
            usermap.put(IRCConstants.HEADER_NICK, u.getNick());
            usermap.put(IRCConstants.HEADER_HOST, u.getHost());
            return usermap;
        }

        private void send(String message, Map<String, String> headers) {
            ChannelProcessor channelProcessor = getChannelProcessor();
            sourceCounter.addToEventReceivedCount(1);
            sourceCounter.incrementAppendBatchReceivedCount();
            channelProcessor.processEvent(EventBuilder.withBody(message,
                    Charsets.UTF_8, headers));
            sourceCounter.addToEventAcceptedCount(1);
            sourceCounter.incrementAppendBatchAcceptedCount();
            headers.clear();
        }
    }

    @Override
    public void configure(Context context) {
        hostname = context.getString(CONF_HOST);
        port = context.getInteger(CONF_PORT, DEFAULT_PORT);
        nick = context.getString(CONF_NICK);
        channels = Arrays.asList(context.getString(CONF_CHANNELS).split(","));
        password = context.getString(CONF_PASSWORD);
        user = context.getString(CONF_USER, NAME_PREFIX + new Random().nextInt(Integer.MAX_VALUE));
        name = context.getString(CONF_NAME, user);
        replyPing = context.getBoolean(CONF_REPLYPING, DEFAULT_REPLYPING);

        Preconditions.checkState(hostname != null, "No hostname specified");
        Preconditions.checkState(nick != null, "No nick specified");
        Preconditions.checkState(!channels.isEmpty(), "No channels specified");

        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
    }

    @Override
    public synchronized void start() {

        logger.info("IRC source starting");

        try {
            createConnection();
        } catch (Exception e) {
            logger.error("Unable to create irc client using hostname:"
                    + hostname + " port:" + port + ". Exception follows.", e);

            destroyConnection();

            return;
        }
        super.start();
    }

    @Override
    public synchronized void stop() {
        logger.info("IRC source {} stopping", this.getName());

        destroyConnection();

        super.stop();

        logger.debug("IRC source {} stopped.", this.getName());

    }

    private void createConnection() throws IOException, InterruptedException {
        if (connection == null) {
            logger.debug(
                    "Creating new connection to hostname:{} port:{}",
                    hostname, port);
            connection = new IRCConnection(hostname, new int[] { port },
                    password, nick, user, name);
            connection.addIRCEventListener(new IRCConnectionListener());
            connection.setEncoding("UTF-8");
            connection.setPong(true);
            connection.setDaemon(false);
            connection.setColors(false);
            connection.connect();

            for (String chan : channels) {
                connection.doJoin(IRC_CHANNEL_PREFIX + chan);
            }
        }
    }

    private void destroyConnection() {
        if (connection != null) {
            logger.debug("Destroying connection to: {}:{}", hostname, port);
            connection.close();
        }

        connection = null;
    }

    @VisibleForTesting
    protected SourceCounter getSourceCounter() {
        return sourceCounter;
    }
}
