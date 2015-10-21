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


public class IRCConstants {

    public IRCConstants(){}

    public static final String CONF_HOST = "host";
    public static final String CONF_PORT = "port";
    public static final String CONF_NICK = "nick";
    public static final String CONF_CHANNELS = "channels";
    public static final String CONF_PASSWORD = "password";
    public static final String CONF_USER = "user";
    public static final String CONF_NAME = "name";
    public static final String CONF_REPLYPING = "replyPing";

    public static final Integer DEFAULT_PORT = 6667;
    public static final Boolean DEFAULT_REPLYPING = Boolean.FALSE;

    public static final String IRC_CHANNEL_PREFIX = "#";
    public static final String NAME_PREFIX = "username_";

    public static final String HEADER_TYPE = "type";
    public static final String HEADER_IDENTIFIER = "identifier";
    public static final String HEADER_CHANNEL = "identifier";
    public static final String HEADER_USERNAME = "username";
    public static final String HEADER_SERVERNAME = "username";
    public static final String HEADER_NICK = "nick";
    public static final String HEADER_HOST = "host";
    public static final String HEADER_NICKPASS = "nickpass";
    public static final String HEADER_NEWNICK = "newnick";
    public static final String HEADER_MODE = "mode";
    public static final String HEADER_IRCMODEPARSER = "irc-mode";
    public static final String HEADER_TARGET = "target";
    public static final String HEADER_TRAILING = "trailing";
    public static final String HEADER_PREFIX = "prefix";
    public static final String HEADER_COMMAND = "command";
    public static final String HEADER_MIDDLE = "middle";
    public static final String HEADER_TOPIC = "topic";
    public static final String HEADER_REPLYPING = "true";

}
