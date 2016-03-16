Stratio Json Interceptor
==============================

Interceptor that looking a header or body content of Events, parse a JSON content and applying a JsonPath 
expression, generate multiple events (once by element found after apply the JsonPath query) setting the 
sub-element values as headers.

Use as JsonPath expression reference the following documentation:
https://github.com/jayway/JsonPath

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
	          
    agent.sources.avroSource.interceptors = jsonInterceptor	          
	agent.sources.spoolSource.interceptors.jsonInterceptor.type = com.stratio.ingestion.interceptor.json.JsonInterceptor$Builder
    agent.sources.spoolSource.interceptors.jsonInterceptor.jsonHeader=books
    agent.sources.spoolSource.interceptors.jsonInterceptor.expression=$.bookstore.books[*]
    agent.sources.spoolSource.interceptors.jsonInterceptor.overwriteBody=true
    
	# Describe the sink
	agent.sinks.logSink.type = logger

	# Use a channel which buffers events in file
	agent.channels.c1.type = memory 

	# Bind the source and sink to the channel
	agent.sources.spoolSource.channels = c1
	agent.sinks.logSink.channel = c1
``` 

Using this configuration, and reading a JSON string from a header or body with this format:
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
  "title":"Everyday Italian",
  "author": "Giada De Laurentiis",
  "year": "2005",
  "price": "30.00"

Event #2 - Headers:
  "title":"Harry Potter",
  "author": "J K. Rowling",
  "year": "2005",
  "price": "29.99"

```

If the Event had headers previously those headers will be mantained.

*You can find an example of bookstore json in src/test/resources/ folder*
