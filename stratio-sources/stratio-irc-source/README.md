Stratio IRC Source
==============================

A Stratio Ingestion source to get data from IRC.

Available config parameters:

- host (string, required): target URI
- port (integer): Port. Default: 6667
- nick(string, required): Nickname
- irc-channels (string, required): Comma separated irc-channels without hash. Example: ubuntu, trivial
- user (string): The username. Is used to register the connection
- name(string): The realname. Is used to register the connection
- password(string): Password. Required if you are registered
- replyPing(boolean): Automatically sends pong when receives a ping. Default: False

Sample Flume config
-------------------

The following file describes an example configuration of a flume agent that request metrics from flume web server and log them using a memory channel.

```
# Name the components on this agent
agent.sources = irc
agent.sinks = logSink
agent.channels = c1

# Describe the source
agent.sources.irc.type=com.stratio.ingestion.source.irc.IRCSource
agent.sources.irc.host=irc.freenode.org
agent.sources.irc.port=6667
agent.sources.irc.nick=antnavper
agent.sources.irc.irc-channels=stratiotest
agent.sources.irc.user=stratiogms
agent.sources.irc.name=antnavper
agent.sources.irc.password=oauth:s362vqqdy4rp4ljblybvjhnq9eg3ev
agent.sources.irc.replyPing=false


# Describe the sink
agent.sinks.logSink.type = logger

# Use a channel which buffers events in file
agent.channels.c1.type = memory 

# Bind the source and sink to the channel
agent.sources.irc.channels = c1
agent.sinks.logSink.channel = c1
```

Building Stratio Ingestion IRC Source
-------------------------------

The source is built using Maven:

mvn clean package

