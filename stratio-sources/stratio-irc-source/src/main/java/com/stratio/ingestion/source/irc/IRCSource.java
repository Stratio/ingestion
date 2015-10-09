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

import static com.stratio.ingestion.source.irc.IRCConstants.CONF_CHANNELS;
import static com.stratio.ingestion.source.irc.IRCConstants.CONF_HOST;
import static com.stratio.ingestion.source.irc.IRCConstants.CONF_NAME;
import static com.stratio.ingestion.source.irc.IRCConstants.CONF_NICK;
import static com.stratio.ingestion.source.irc.IRCConstants.CONF_PASSWORD;
import static com.stratio.ingestion.source.irc.IRCConstants.CONF_PORT;
import static com.stratio.ingestion.source.irc.IRCConstants.CONF_REPLYPING;
import static com.stratio.ingestion.source.irc.IRCConstants.CONF_USER;
import static com.stratio.ingestion.source.irc.IRCConstants.DEFAULT_PORT;
import static com.stratio.ingestion.source.irc.IRCConstants.DEFAULT_REPLYPING;
import static com.stratio.ingestion.source.irc.IRCConstants.IRC_CHANNEL_PREFIX;
import static com.stratio.ingestion.source.irc.IRCConstants.NAME_PREFIX;

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
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.schwering.irc.lib.IRCConnection;
import org.schwering.irc.lib.IRCEventListener;
import org.schwering.irc.lib.IRCModeParser;
import org.schwering.irc.lib.IRCUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 *
 * Connect IRC channels and retrieve messages from them.
 *
 * Configuration parameters are:
 *
 * <p>
 * <ul>
 * <li><tt>hostname</tt> <em>(string, required)</em>: target URI.</li>
 * <li><tt>port</tt> <em>(integer)</em>: Port. Default: 6667 .</li>
 * <li><tt>nick</tt> <em>(string, required)</em>: Nickname.</li>
 * <li><tt>channels</tt> <em>(string, required)</em>: Comma separated channels without hash. Example: ubuntu,
 * trivial. </li>
 * <li><tt>user</tt> <em>(string)</em>: The username. Is used to register the connection.</li>
 * <li><tt>name</tt> <em>(string)</em>: The realname. Is used to register the connection.</li>
 * <li><tt>password</tt> <em>(string)</em>: Password. Required if you are registered.</li>
 * <li><<tt>replyPing</tt> <em>(boolean)</em>: Automatically sends pong when receives a ping. Default: False.</li>
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

        /**
         * @param
         * @return void
         */
        public void onRegistered() {
            logger.info("IRC on Registered called");
            headers.put(IRCConstants.HEADER_TYPE, "registered");
            send("", headers);
        }

        /**
         * @param
         * @return void
         */
        public void onDisconnected() {
            logger.error("IRC source disconnected");
            headers.put(IRCConstants.HEADER_TYPE, "disconnected");
            send("", headers);
        }

        /**
         * @param msg
         * @return void
         */
        public void onError(String msg) {
            logger.error("IRC source error: {}", msg);
            headers.put(IRCConstants.HEADER_TYPE, "error");
            send(msg, headers);
        }

        /**
         * @param msg
         * @param num
         * @return void
         */
        public void onError(int num, String msg) {
            logger.debug("IRC source error: {} - {}", num, msg);
            headers.put(IRCConstants.HEADER_TYPE, "error");
            headers.put(IRCConstants.HEADER_IDENTIFIER, String.valueOf(num));
            send(msg, headers);
        }

        /**
         * @param chan
         * @param user
         * @return void
         */
        public void onInvite(String chan, IRCUser user, String nickPass) {
            logger.debug("User {} was invited to channel {}.", user.getNick(), chan);
            headers.put(IRCConstants.HEADER_TYPE, "invite");
            headers.put(IRCConstants.HEADER_CHANNEL, chan);
            headers.putAll(getUser(user));
            send("", headers);
        }

        /**
         * @param chan
         * @param user
         * @return void
         */
        public void onJoin(String chan, IRCUser user) {
            logger.debug("User {} joined to channel {}.", user.getNick(), chan);
            headers.put(IRCConstants.HEADER_TYPE, "join");
            headers.put(IRCConstants.HEADER_CHANNEL, chan);
            headers.putAll(getUser(user));
            send("", headers);
        }

        /**
         * @param chan
         * @param user
         * @param nickPass
         * @param msg
         * @return void
         */
        public void onKick(String chan, IRCUser user, String nickPass, String msg) {
            logger.debug("User {} was kicked from channel {}.", user.getNick(), chan);
            headers.put(IRCConstants.HEADER_TYPE, "kick");
            headers.put(IRCConstants.HEADER_CHANNEL, chan);
            headers.putAll(getUser(user));
            headers.put(IRCConstants.HEADER_NICKPASS, nickPass);
            send(msg, headers);
        }

        /**
         * @param nickPass
         * @param user
         * @param mode
         * @return void
         */
        public void onMode(IRCUser user, String nickPass, String mode) {
            logger.debug("User {} changed mode.", user.getNick());
            headers.put(IRCConstants.HEADER_TYPE, "mode");
            headers.putAll(getUser(user));
            headers.put(IRCConstants.HEADER_NICKPASS, nickPass);
            headers.put(IRCConstants.HEADER_MODE, mode);
            send("", headers);
        }

        /**
         * @param chan
         * @param user
         * @param mp
         * @return void
         */
        public void onMode(String chan, IRCUser user, IRCModeParser mp) {
            logger.debug("User {} changed {} channel mode.", user.getNick(), chan);
            headers.put(IRCConstants.HEADER_TYPE, "mode");
            headers.put(IRCConstants.HEADER_CHANNEL, chan);
            headers.putAll(getUser(user));
            headers.put(IRCConstants.HEADER_IRCMODEPARSER, mp.toString());
            send("", headers);
        }

        /**
         * @param nickNew
         * @param user
         * @return void
         */
        public void onNick(IRCUser user, String nickNew) {
            logger.debug("User {} changed his nick to {} channel mode.", user.getNick(), nickNew);
            headers.put(IRCConstants.HEADER_TYPE, "nick");
            headers.putAll(getUser(user));
            headers.put(IRCConstants.HEADER_NEWNICK, user.getNick());
            send("", headers);
        }

        /**
         * @param target
         * @param user
         * @param msg
         * @return void
         */
        public void onNotice(String target, IRCUser user, String msg) {
            logger.debug("User {}  noticed {} to {}.", user.getNick(), msg, target);
            headers.put(IRCConstants.HEADER_TYPE, "notice");
            headers.putAll(getUser(user));
            headers.put(IRCConstants.HEADER_TARGET, target);
            send(msg, headers);
        }

        /**
         * @param chan
         * @param user
         * @param msg
         * @return void
         */
        public void onPart(String chan, IRCUser user, String msg) {
            logger.debug("User {} is part of channel {}.", user.getNick(), chan);
            headers.put(IRCConstants.HEADER_TYPE, "part");
            headers.put(IRCConstants.HEADER_CHANNEL, chan);
            headers.putAll(getUser(user));
            send(msg, headers);
        }

        /**
         * @param chan
         * @param user
         * @param msg
         * @return void
         */
        public void onPrivmsg(String chan, IRCUser user, String msg) {
            logger.debug("User {} wrote a message to {} - {}", user.getNick(), chan, msg);
            headers.put(IRCConstants.HEADER_TYPE, "privmsg");
            headers.put(IRCConstants.HEADER_CHANNEL, chan);
            headers.putAll(getUser(user));
            send(msg, headers);
        }

        /**
         * @param msg
         * @param user
         * @return void
         */
        public void onQuit(IRCUser user, String msg) {
            logger.debug("User {} left the chat.");
            headers.put(IRCConstants.HEADER_TYPE, "quit");
            headers.putAll(getUser(user));
            send(msg, headers);
        }

        /**
         * @param msg
         * @param num
         * @param value
         * @return void
         */
        public void onReply(int num, String value, String msg) {
            logger.debug("A numeric reply with identifier {} and value {} was received.", num, value);
            headers.put(IRCConstants.HEADER_TYPE, "reply");
            send(msg, headers);
        }

        /**
         * @param chan
         * @param user
         * @param topic
         * @return void
         */
        public void onTopic(String chan, IRCUser user, String topic) {
            logger.debug("User {} changed {} channel topic to {}", user.getNick(), chan, topic);
            headers.put(IRCConstants.HEADER_TYPE, "topic");
            headers.putAll(getUser(user));
            headers.put(IRCConstants.HEADER_CHANNEL, chan);
            headers.put(IRCConstants.HEADER_TOPIC, topic);
            send(topic, headers);
        }

        /**
         * @param ping
         * @return void
         */
        public void onPing(String ping) {
            logger.debug("Ping {}." + ping);
            headers.put(IRCConstants.HEADER_TYPE, "ping");
            headers.put(IRCConstants.HEADER_TYPE, "unknown");
            send(ping, headers);

            if(replyPing){
                connection.doPong(ping);
            }
        }

        /**
         * @param prefix
         * @param command
         * @param middle
         * @param trailing
         * @return void
         */
        public void unknown(String prefix, String command, String middle, String trailing) {
            logger.debug("Unknown event was received. ");
            headers.put(IRCConstants.HEADER_TYPE, "unknown");
            headers.put(IRCConstants.HEADER_TRAILING, trailing);
            headers.put(IRCConstants.HEADER_PREFIX, middle);
            headers.put(IRCConstants.HEADER_COMMAND, command);
            headers.put(IRCConstants.HEADER_MIDDLE, middle);
            send("", headers);
        }

        /**
         * @param user
         * @return void
         */
        private Map<String, String> getUser(IRCUser user) {
            Map<String, String> usermap = new HashMap();
            usermap.put(IRCConstants.HEADER_USERNAME, user.getUsername());
            usermap.put(IRCConstants.HEADER_SERVERNAME, user.getServername());
            usermap.put(IRCConstants.HEADER_NICK, user.getNick());
            usermap.put(IRCConstants.HEADER_HOST, user.getHost());
            return usermap;
        }

        /**
         * @param message
         * @param headers
         * @return void
         */
        private void send(String message, Map<String, String> headers) {
            ChannelProcessor channelProcessor = getChannelProcessor();
            sourceCounter.addToEventReceivedCount(1);
            sourceCounter.incrementAppendBatchReceivedCount();
//            channelProcessor.processEvent(EventBuilder.withBody(message,
//                    Charsets.UTF_8, headers));
            sourceCounter.addToEventAcceptedCount(1);
            sourceCounter.incrementAppendBatchAcceptedCount();
            headers.clear();
        }

    }

    public IRCConnectionListener getIRCConnectionListener(){
        return new IRCConnectionListener();
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

    public void setHeaders() {
        IRCSource.IRCConnectionListener ircConnectionListener = this.getIRCConnectionListener();
        IRCUser ircUser = new IRCUser(nick, user, hostname);
        IRCModeParser ircModeParser = new IRCModeParser("line");
        ircConnectionListener.onDisconnected();
        ircConnectionListener.onError("message");
        ircConnectionListener.onError(14, "message");
        ircConnectionListener.onInvite("chan", ircUser, "nick");
        ircConnectionListener.onJoin("chan", ircUser);
        ircConnectionListener.onKick("chan", ircUser, "nick", "message");
        ircConnectionListener.onMode(ircUser, "nick", "mode");
        ircConnectionListener.onMode("chan", ircUser, ircModeParser);
        ircConnectionListener.onNick(ircUser, "nick");
        ircConnectionListener.onNotice("target", ircUser, "message");
        ircConnectionListener.onPart("chan", ircUser, "message");
        ircConnectionListener.onPing("ping");
        ircConnectionListener.onPrivmsg("chan", ircUser, "message");
        ircConnectionListener.onQuit(ircUser, "message");
        ircConnectionListener.onRegistered();
        ircConnectionListener.onReply(12, "value", "message");
        ircConnectionListener.onTopic("chan", ircUser, "topic");
        ircConnectionListener.unknown("prefix", "command", "middle", "trailing");
        Map<String, String> user = ircConnectionListener.getUser(ircUser);
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
