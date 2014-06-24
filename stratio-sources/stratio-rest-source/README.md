Stratio REST Source
==============================

A Flume source using Stratio Streaming.

The Stratio Request Metrics Source will 

Available config parameters:

- url: target URL request. Required.
- method: Method type. Default: GET.
- applicationType: Application Type. Default: JSON. Possible values: TEXT, JSON.
- bodyField: Field to include in body. Default: None.
- frequency: Frequency to send request to the url in seconds. Default: 10.


This source will send a request to url target every 'frequency' seconds, retrieve the response and put it into its flume channel.

Sample Flume config
-------------------

The following file describes an example configuration of a flume agent that request metrics from flume web server and log them using a memory channel.


# Name the components on this agent
agent.sources = requestMetrics
agent.sinks = logSink
agent.channels = c1

# Describe the source
agent.sources.requestMetrics.type=com.stratio.ingestion.source.rest.RestSource
agent.sources.requestMetrics.url=http://localhost:34545/metrics
agent.sources.requestMetrics.method=GET
agent.sources.requestMetrics.applicationType=JSON
agent.sources.requestMetrics.frequency=10


# Describe the sink
agent.sinks.logSink.type = logger

# Use a channel which buffers events in file
agent.channels.c1.type = memory 

# Bind the source and sink to the channel
agent.sources.requestMetrics.channels = c1
agent.sinks.logSink.channel = c1


Building Stratio REST Source
-------------------------------

The source is built using Maven:

mvn clean package

Run example
-------------------------------

Copy .jar resuling from package and run agent on flume

bin/flume-ng agent --conf conf/ -f agent.conf -n agent -Dflume.monitoring.type=http -Dflume.monitoring.port=34545
