Stratio Decision Sink
==============================

A Flume sink using Stratio Decision.

The Stratio Decision Sink will insert flume events to an specific stream. The configuration is located in the flume config (see sample below.)

Available config parameters:

- kafka: Kafka brokers (comma separated list) where the Stratio Decision/Kafka instance is running
- zookeeper: Zookeeper quorum where the Stratio Decision/Zookeeper instance is running
- streamDefinitionFile: stream definition file path (see example below)

This sink will extract the data from the flume event headers and for each field within the headers map it will create a new stream "field" with the content of the specific header. You must provide a mechanism to parse the content of the flume flow to the event headers (we strongly recommend using morphlines).


Sample Flume config
-------------------

The following file describes an example configuration of a flume agent that uses a Spooling directory source, a file channel our Stratio Decision Sink and one morphline interceptor which parses the flume flow content to the events header

```properties
# Name the components on this agent
agent.sources = r1
agent.sinks = decisionSink
agent.channels = c1

# Describe/configure the source
agent.sources.r1.type = spoolDir
agent.sources.r1.spoolDir = /home/flume/data/files

# Describe the sink
agent.sinks.decisionSink.type=com.stratio.ingestion.sink.decision.StratioDecisionSink
agent.sinks.decisionSink.kafka=localhost:9092
agent.sinks.decisionSink.zookeeper=localhost:2181
agent.sinks.decisionSink.streamDefinitionFile=/path/to/stream/definition/file/stream.conf

# Define the interceptors
agent.sources.r1.interceptors = morphlineinterceptor
agent.sources.r1.interceptors.morphlineinterceptor.type = org.apache.flume.sink.solr.morphline.MorphlineInterceptor$Builder
agent.sources.r1.interceptors.morphlineinterceptor.morphlineFile = /home/flume/conf/interceptor.conf
agent.sources.r1.interceptors.morphlineinterceptor.morphlineId = morphline1

# Use a channel which buffers events in file
agent.channels.c1.type = file
agent.channels.c1.checkpointDir = /home/flume/channel/check/
agent.channels.c1.dataDirs = /home/flume/channel/data/
agent.channels.c1.transactionCapacity=10000

# Bind the source and sink to the channel
agent.sources.r1.channels = c1
agent.sinks.decisionSink.channel = c1

```

Stream definition file
----------------------

See following an stream definition file. The available stream field types are:

   - string
   - boolean
   - double
   - integer
   - long
   - float

```
{
    "streamName": "testStream",
    "fields": [
        {"name" : "field1","type" : "string"},
        {"name" : "field2","type" : "integer"}
        ]
}
```

Morphline definition file
------------------------

This file is an example of a morphline file that parses the content of the flume flow:

```
morphlines : [
  {
    # Morphline identifier
    id : morphline1

    # Paquete donde se encuentran los comandos a utilizar
    importCommands : ["org.kitesdk.**"]

    # Command list
    commands : [

      # readLine command.
      # Setting utf-8 as our encoding
      {
        readLine {
          charset : utf-8
        }
      }

      # generateUUID command.
      # The UUID will be stored in the log_id field
      {
        generateUUID {
          field : log_id
        }
      }

      # grok command.
      # We should define where the dictionaries location.
      # We define the regular expression to be used.
      {
        grok {
          dictionaryFiles : [/path/to/grok-dictionaries]
          expressions : {
            message : """%{IPORHOST:log_host} - %{USER:log_user} \[%{HTTPDATE:log_date}\] \"%{DATA:log_http_method} %{DATA:log_url_path}                             %{DATA:log_http_version}\" %{NONNEGINT:log_http_code} %{DATA:log_bytes_returned}"""
          }
        }
      }
    ]
  }
]
```

Building Stratio Decision Sink
------------------------------

The sink is built using Maven:

mvn clean package
