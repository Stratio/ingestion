Stratio JSON XPath Deserializer
==============================

Deserializer that take json files, apply an XPath expression to it and emit events according the NodeList result of
this.

Sample Flume config
=================================


``` 
    	# Name the components on this agent
	agent.sources = spoolSource
	agent.sinks = logSink
	agent.channels = c1

	# Describe the source
	agent.sources.spoolSource.type=spooldir
	agent.sources.spoolSource.spoolDir=/home/stratio/spool/
	agent.sources.spoolSource.batchSize=100
	agent.sources.spoolSource.deserializer=com.stratio.ingestion.deserializer.jsonxpath.JsonXpathDeserializer$Builder
	agent.sources.spoolSource.deserializer.expression=/bookstore/book
	agent.sources.spoolSource.deserializer.outputField=element
	agent.sources.spoolSource.deserializer.headers.address=/bookstore/address
	agent.sources.spoolSource.deserializer.headers.owner=/bookstore/owner

	# Describe the sink
	agent.sinks.logSink.type = logger

	# Use a channel which buffers events in file
	agent.channels.c1.type = memory 

	# Bind the source and sink to the channel
	agent.sources.spoolSource.channels = c1
	agent.sinks.logSink.channel = c1
``` 

*You can find an example of bookstore xml in src/test/resources/ folder*
