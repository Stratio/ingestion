Stratio Random Source
==============================

A Flume source to generate random data.

Available config parameters:

- generatorDefitionFile (required): random generator definition file path (see example below)
- maxEventsNumber (optional): allows the user to limit the number of generated events.

Random generator definition file
---------------------

See following a generator definition file. The available generator fields are:

 * string
 * integer
 * list
 * date

```json
{
    "fields": [
        {"type" : "list",
         "properties": [
                         {
                             "propertyName": "values",
                             "propertyValue": "value1, value2, value3"
                         }
                     ]
         },
        {"type" : "integer",
        "properties": [
                        {
                            "propertyName": "length",
                            "propertyValue": "4"
                        }
                    ]
        },
        {"type" : "string",
         "properties": [
                         {
                             "propertyName": "length",
                             "propertyValue": "5"
                         }
                     ]
        },
        {"type" : "date",
                 "properties": [
                                 {
                                     "propertyName": "dateFormat",
                                     "propertyValue": "dd/MMM/yyyy:hh:mm:ss Z"
                                 }
                             ]
                }
        ]
}
```

Defining this configuration file the source will generate events with the following pattern:

- value1 1234 abcde 01/Jul/1995:00:00:01 -0400
- value2 5678 fghij 01/Jul/1995:00:00:02 -0400
- value3 9087 lmhkd 01/Jul/1995:00:00:02 -0400
......


Sample Flume config
-------------------

The following file describes an example configuration of a flume agent that generate events using our generate random source and log them using a file channel.

```
# Name the components on this agent
agent.sources = r1
agent.sinks = logSink
agent.channels = c1
agent.sources.r1.channels = c1

# Describe/configure the source
agent.sources.r1.type = com.stratio.ingestion.source.generator.RandomGeneratorSource
agent.sources.r1.generatorDefinitionFile = generator.conf

# Describe the sink
agent.sinks.logSink.type = logger

# Use a channel which buffers events in file
agent.channels.c1.type = file
agent.channels.c1.checkpointDir = /home/albertorodriguez/flume/channel/check/
agent.channels.c1.dataDirs = /home/albertorodriguez/flume/channel/data/
agent.channels.c1.transactionCapacity=10000

# Bind the source and sink to the channel
agent.sinks.logSink.channel = c1

```

Building Stratio Random Source
-------------------------------

The source is built using Maven:

mvn clean package

Run example
-------------------------------

Copy .jar resuling from package and run agent on flume

bin/flume-ng agent --conf conf --conf-file streaming-random.conf --name agent -Dflume.root.logger=INFO,console -Dflume.monitoring.type=http
