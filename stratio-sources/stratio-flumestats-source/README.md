Stratio Flume Stats Source
==============================

A Flume source that retrieves stats from Flume web server.

Available config parameters:

- host: Flume webserver host. Default: localhost.
- port: Flume webserver port. Default: 41414.
- frequency: Frequency to send request to the url in seconds. Default: 10.

This source will send a request to flume stats web server target every 'frequency' seconds, retrieve the response, parse it and send an event for each flume component. 


Complete Example
-------------------

The following file describes an example configuration of a flume agent that request metrics from flume web server, format timestamps and index on ElasticSearch using the official ES sink and Stratio ES Serializer.

```
# Name the components on this agent
agent.sources = requestMetrics
agent.sinks = esearch
agent.channels = memoryChannel

# Describe/configure the source
agent.sources.requestMetrics.type = com.stratio.ingestion.source.flumestats.FlumeStatsSource
agent.sources.requestMetrics.host=localhost
agent.sources.requestMetrics.port=34545
agent.sources.requestMetrics.frequency=5

#Interceptor change timestamps
agent.sources.requestMetrics.interceptors = morphline
agent.sources.requestMetrics.interceptors.morphline.type = org.apache.flume.sink.solr.morphline.MorphlineInterceptor$Builder
agent.sources.requestMetrics.interceptors.morphline.morphlineFile = morphlineMetrics.conf
agent.sources.requestMetrics.interceptors.morphline.morphlineId = morphline1

# Describe the sink
agent.sinks.esearch.type = org.apache.flume.sink.elasticsearch.ElasticSearchSink
agent.sinks.esearch.indexName = metrics
agent.sinks.esearch.hostNames = localhost
agent.sinks.esearch.serializer = com.stratio.ingestion.deserializer.elasticsearch.ElasticSearchSerializerWithMapping
agent.sinks.esearch.serializer.mappingFile = mapping.json

# Use a channel which buffers events in file
agent.channels = memoryChannel
agent.channels.memoryChannel.type = memory

# Bind the source and sink to the channel
agent.sources.requestMetrics.channels = memoryChannel
agent.sinks.esearch.channel = memoryChannel
```

morphlineMetrics.conf
---------------------

Configuration file for morphline.

```
morphlines : [
  {
    id : morphline1

    importCommands : ["org.kitesdk.**", "com.stratio.ingestion.morphline.**"]

    commands : [
      {
        convertTimestamp {
	  field : StartTime
	  inputFormats : ["unixTimeInMillis"]
	  outputFormat : "yyyyMMdd'T'HHmmssZ"
	  outputTimezone : UTC
	}
      }
      {
        convertTimestamp {
	  field : StopTime
	  inputFormats : ["unixTimeInMillis"]
	  outputFormat : "yyyyMMdd'T'HHmmssZ"
	  outputTimezone : UTC
	}
      }
    ] 

  }
]
```


mapping.json
------------

Configuration file for ElasticSearchSerializer

```
{ 
    "properties" : {
        "Name": {
            "type": "string"
        },
        "Type": {
             "type": "string"
        },
	"AppendBatchAcceptedCount": {
	     "type": "long"
	},
	"StopTime": {
	     "type": "date",
             "format": "basic_date_time_no_millis"
	},
	"StartTime": {
	     "type": "date",
             "format": "basic_date_time_no_millis"
	},
	....
	....	
    }
}
```
