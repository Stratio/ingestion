Stratio Redis Source
==============================

A Flume source that read data from Redis Pub Sub system.

Configuration
=============

Available config parameters:

- `host` *(string)*: Redist host. Default: localhost.
- `port` *(integer)*: Redis port. Default: 6379.
- `channels` *(string)*: Channels to subscribe to. String or comma separated strings. Required.
- `charset`*(string)* : Charset. Default: uft-8.


Sample Complete-flow Flume config
=================================

The following paragraph describes an example configuration of an Flume agent that uses our Redis source, a [File channel](http://flume.apache.org/FlumeUserGuide.html#file-channel) and print events in [Logger Sink](https://flume.apache.org/FlumeUserGuide.html#logger-sink).

```
# Name the components on this agent
agent.sources = pubsub
agent.sinks = logSink
agent.channels = c1

# Describe the source
agent.sources.pubsub.type=com.stratio.ingestion.source.redis.RedisSource
agent.sources.pubsub.host=localhost
agent.sources.pubsub.channels=channel1, channel2


# Describe the sink
agent.sinks.logSink.type = logger


# Use a channel which buffers events in file
agent.channels.c1.type = file
agent.channels.c1.checkpointDir = /home/user/flume/channel/check/
agent.channels.c1.dataDirs = /home/user/flume/channel/data/
agent.channels.c1.transactionCapacity=10000


# Bind the source and sink to the channel
agent.sources.pubsub.channels = c1
agent.sinks.logSink.channel = c1
```
