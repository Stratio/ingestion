Stratio Redis Source
==============================

A Flume source that read data from Redis Pub Sub system with connection Pool. Accept patterns.

Configuration
=============

Available config parameters:

- `host` *(string)*: Redist host. Default: localhost.
- `port` *(integer)*: Redis port. Default: 6379.
- `subscribe` *(string)*: Channels to subscribe to. String or comma separated strings.
- `psubscribe` *(string)*: Channels to subscribe with given pattern. String or comma separated strings. Invalid if subscribe is assigned.
- `charset`*(string)* : Charset. Default: uft-8.
- `pool.<property>`: Prefix for pool properties. Set whatever property you want to the connection pool.



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
agent.sources.pubsub.subscribe=channel1,channel2
agent.sources.pubsub.pool.testOnBorrow=true


# Describe the sink
agent.sinks.logSink.type = logger


# Use a channel which buffers events in file
agent.channels.c1.type = memory

# Bind the source and sink to the channel
agent.sources.pubsub.channels = c1
agent.sinks.logSink.channel = c1
```
