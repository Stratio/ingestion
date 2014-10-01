Stratio Kafka Sink
====================

Kafka Sink.  Send events to Kafka.

Configuration
=============

The available config parameters are:

- `topic` *(string)*:  Name of topic where event will be sent to. Required.
- `writeBody` *(boolean)*: true to send body in raw String format and false to send headers in json String format. Default: False (Send only headers).
- `kafka.<producer-property>` *(string)*: This sink accept any kafka producer property. Just write it after prefix "kafka.". Example: kafka.metadata.broker.list



Sample Complete-flow Flume config
=================================

The following file describes an example configuration of an Flume agent that uses a [Spooling directory source](http://flume.apache.org/FlumeUserGuide.html#spooling-directory-source), a [File channel](http://flume.apache.org/FlumeUserGuide.html#file-channel) and our Kafka Sink

``` 
    # Name the components on this agent
    agent.sources = r1
    agent.sinks = kafkaSink
    agent.channels = c1

    # Describe/configure the source
    agent.sources.r1.type = spoolDir
    agent.sources.r1.spoolDir = /home/flume/data/files/

    # Describe the sink
    agent.sinks.kafkaSink.type = com.stratio.ingestion.sink.kafka.KafkaSink
    agent.sinks.kafkaSink.topic = test
    agent.sinks.kafkaSink.kafka.metadata.broker.list = localhost:9092
    agent.sinks.kafkaSink.kafka.serializer.class = kafka.serializer.StringEncoder

    # Use a channel which buffers events in file
    agent.channels.c1.type = file
    agent.channels.c1.checkpointDir = /home/user/flume/channel/check/
    agent.channels.c1.dataDirs = /home/user/flume/channel/data/
    agent.channels.c1.transactionCapacity=10000

    # Bind the source and sink to the channel
    agent.sources.r1.channels = c1
    agent.sinks.kafkaSink.channel = c1
```
