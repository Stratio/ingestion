Stratio ElasticSearch Sink
==========================

ElasticSearch sink. Send events to ElasticSearch.

We create that sink to solve a bug in the parse of json files that it is solving in Flume 1.7.

Available config parameters:

- channel	    –	 
- type	        –	The component type name, needs to be org.apache.flume.sink.elasticsearch.ElasticSearchSink
- hostNames	    –	Comma separated list of hostname:port, if the port is not present the default port ‘9300’ will be used
- indexName	    - 	The name of the index which the date will be appended to. Example ‘flume’ -> ‘flume-yyyy-MM-dd’ Arbitrary header substitution is supported, eg. %{header} replaces with value of named event header
- indexType	    - 	The type to index the document to, defaults to ‘log’ Arbitrary header substitution is supported, eg. %{header} replaces with value of named event header
- clusterName	- 	Name of the ElasticSearch cluster to connect to
- batchSize	    - 	Number of events to be written per txn.
- ttl	        –	TTL in days, when set will cause the expired documents to be deleted automatically, if not set documents will never be automatically deleted. TTL is accepted both in the earlier form of integer only e.g. a1.sinks.k1.ttl = 5 and also with a qualifier ms (millisecond), s (second), m (minute), h (hour), d (day) and w (week). Example a1.sinks.k1.ttl = 5d will set TTL to 5 days. Follow http://www.elasticsearch.org/guide/reference/mapping/ttl-field/ for more information.
- serializer	- 	The ElasticSearchIndexRequestBuilderFactory or ElasticSearchEventSerializer to use. Implementations of either class are accepted but ElasticSearchIndexRequestBuilderFactory is preferred.
- serializer.*	–	Properties to be passed to the serializer.

This sink will extract the data from the flume event headers and for each field within the headers map it will create a new stream "field" with the content of the specific header. You must provide a mechanism to parse the content of the flume flow to the event headers (we strongly recommend using morphlines).

For more information (http://flume.apache.org/FlumeUserGuide.html#elasticsearchsink)

Sample Flume config
-------------------

The following file describes an example configuration of a flume agent that uses a Spooling directory source, a file channel our Stratio ElasticSearch Sink and one morphline interceptor which parses the flume flow content to the events header

```properties
# Name the components on this agent
agent.sources = r1
agent.sinks = elasticSink
agent.channels = c1

# Describe/configure the source
agent.sources.r1.type = spoolDir
agent.sources.r1.spoolDir = /home/flume/data/files

# Describe the sink
agent.sinks.elasticSink.type=com.stratio.ingestion.sink.elasticsearch.ElasticSearchSink
agent.sinks.elasticSink.hostNames=localhost:9200,localhost:9300

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
agent.sinks.elasticSink.channel = c1

```

