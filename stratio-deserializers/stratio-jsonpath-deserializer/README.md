Stratio JsonPath Deserializer
==============================

Deserializer that take Json files, apply an JPath expression to it and emit events according the NodeList result of 
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
	agent.sources.spoolSource.deserializer=com.stratio.ingestion.deserializer.jsonpath.JsonpathDeserializer$Builder
	agent.sources.spoolSource.deserializer.expression=$.bookstore.books[*]
	agent.sources.spoolSource.deserializer.outputBody=true
	agent.sources.spoolSource.deserializer.rootElementsExpression=$.bookstore
	
	# Describe the sink
	agent.sinks.logSink.type = logger

	# Use a channel which buffers events in file
	agent.channels.c1.type = memory 

	# Bind the source and sink to the channel
	agent.sources.spoolSource.channels = c1
	agent.sinks.logSink.channel = c1
``` 

Using this configuration, and reading a JSON file with this format:
```
{
  "bookstore": {
    "numberOfTitles": 159,
    "city": "Barcelona",
    "books": [
      {
        "title":"Everyday Italian",
        "author": "Giada De Laurentiis",
        "year": "2005",
        "price": "30.00"
      },
      {
        "title":"Harry Potter",
        "author": "J K. Rowling",
        "year": "2005",
        "price": "29.99"
      }
    ]    
  }
}
```

We will generate from one event two events with the following format:
```
Event #1 - Headers:
  "numberOfTitles": 159,
  "city": "Barcelona",
  "title":"Everyday Italian",
  "author": "Giada De Laurentiis",
  "year": "2005",
  "price": "30.00"

Event #2 - Headers:
  "numberOfTitles": 159,
  "city": "Barcelona",
  "title":"Harry Potter",
  "author": "J K. Rowling",
  "year": "2005",
  "price": "29.99"

```

*You can find an example of bookstore json in src/test/resources/ folder*
