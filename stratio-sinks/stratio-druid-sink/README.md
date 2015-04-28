Stratio Druid Sink
====================

The Druid Sink component allows to save Flume-flow events to Druid.


Configuration
=============

The available config parameters are:

- `indexService` *(String, required)*: Overlord's service name

- `firehosePattern` *(String)*:  Firehoses describe the data stream source. Make up a service pattern, include %s 
somewhere in it. This will be used for internal service-discovery purposes, to help druid sink find Druid indexing 
tasks. By default, *druid:firehose:%s*.

- `discoveryPath` *(String, required)*: Your overlord's druid.discovery.curator.path

- `dataSource` *(String)*: Source name where events will be stored. Very similar to a table in relational databases. 
 By default, *sampleSource*.

- `dimensions` *(String, required)*: Comma separated list with event headers you want to stored. Similar to columns in 
relational databases. 

- `aggregators` *(String)*: Different specifications of processing over available metrics. By default, *count* 
aggregator.

- `zookeeperLocation` *(String)*: Zookeeper location (hostname:port).By default, *127.0.0.1:2181*.

- `timestampField` *(String)*: The field name where event timestamp info is extracted from. By default, *timestamp*.

- `segmentGranularity` *(Granularity)*: Time granularity (minute, hour, day, week, month) for loading data at query 
time. Recommended, more than queryGranularity.  By default, *HOUR*.

- `queryGranularity` *(Granularity)*: Time granularity (minute, hour, day, week, month) for rollup. At least, less 
than segmentGranularity. Recommended: minute, hour, day, week, month.  By default, *NONE*.

- `period` *(Period)*: While reading, events with timestamp older than now minus this value, will be discarded.  By 
default, *PT10M*.

- `partitions` *(Integer)*: This is used to scale ingestion up to handle larger streams.  By default, *1*.

- `replicants` *(Integer)*: This is used to provide higher availability and parallelism for queries.  By default, *1*.

- `baseSleepTime` *(Integer)*: Initial amount of time to wait between retries.  By default, *1000*.

- `maxRetries` *(Integer)*: Max number of times to retry.  By default, *3*.

- `maxSleep` *(Integer)*: Max time in ms to sleep on each retry.  By default, *30000*.

- `batchSize` *(Integer)*: Number of events to batch together to be send to our data source.  By default, *1000*.

As a recommendation: The normal, expected use cases have the following overall constraints: queryGranularity < 
windowPeriod < segmentGranularity.

Sample Complete-flow Flume config
=================================

The following file describes an example configuration of an Flume agent that uses a [Syslog source]
(http://flume.apache.org/FlumeUserGuide.html#syslog-tcp-source), a [File channel](http://flume.apache
.org/FlumeUserGuide.html#file-channel) and our Druid Sink


    # Name the components on this agent
    agent.sources = logs
    agent.sinks = druidSink
    agent.channels = c1
    
    
    # Describe the source - Syslog
    agent.sources.logs.type = syslogtcp
    agent.sources.logs.port = 5140
    agent.sources.logs.host = 127.0.0.1
    
    #Interceptor morphline
    agent.sources.logs.interceptors = morphline
    agent.sources.logs.interceptors.morphline.type = org.apache.flume.sink.solr.morphline.MorphlineInterceptor$Builder
    agent.sources.logs.interceptors.morphline.morphlineFile = morphline.conf
    agent.sources.logs.interceptors.morphline.morphlineId = morphline1
    
    # Describe the Druid sink
    agent.sinks.druidSink.type = com.stratio.ingestion.sink.druid.DruidSink
    agent.sinks.druidSink.indexService = druid/prod/overlord
    agent.sinks.druidSink.discoveryPath = /prod/discovery
    agent.sinks.druidSink.dimensions = timestamp,IPAddr,LogLevel,Domain,MessageID
    
    # Use a channel which buffers events in file
    agent.channels.c1.type = file
    agent.channels.c1.checkpointDir = data/channel/check/
    agent.channels.c1.dataDirs = data/channel/data/
    agent.channels.c1.capacity = 1000000
    agent.channels.c1.transactionCapacity = 10000
    agent.channels.c1.checkpointInterval = 300000
    
    # Bind the source and sink to the channel
    agent.sources.logs.channels = c1
    agent.sinks.druidSink.channel = c1
    

