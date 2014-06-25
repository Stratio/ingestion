Stratio Flume Stats Source
==============================

A Flume source that retrieves stats from Flume web server.

Available config parameters:

- host: Flume webserver host. Default: localhost.
- port: Flume webserver port. Default: 41414.
- frequency: Frequency to send request to the url in seconds. Default: 10.

This source will send a request to flume stats web server target every 'frequency' seconds, retrieve the response, parse it and send an event for each flume component. 


Sample Flume config
-------------------

The following file describes an example configuration of a flume agent that request metrics from flume web server and log them using a memory channel.

```
# Name the components on this agent
agent.sources = requestMetrics
agent.sinks = logSink
agent.channels = c1

# Describe the source
agent.sources.requestMetrics.type=com.stratio.ingestion.source.flumestats.FlumeStatsSource
agent.sources.requestMetrics.host=localhost
agent.sources.requestMetrics.port=34545
agent.sources.requestMetrics.frequency=5


# Describe the sink
agent.sinks.logSink.type = logger

# Use a channel which buffers events in file
agent.channels.c1.type = memory 

# Bind the source and sink to the channel
agent.sources.requestMetrics.channels = c1
agent.sinks.logSink.channel = c1
```
