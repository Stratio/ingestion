Stratio REST Source
===================
A Flume source that make a request to a REST service.

Available config parameters:

- url: target URL request. Required.
- method: Method type. Default: GET.
- applicationType: Application Type. Default: JSON. Possible values: TEXT, JSON.
- frequency: Frequency to send request to the url in seconds. Default: 10.
- headers: Headers json. e.g.: { Accept-Charset: utf-8, Date: Tue,15 Nov 1994 08:12:31 GMT} Default: "".
- body: Body for post request. Default: "".
- restSourceHandler: Handlder implementation classname. This handler will transform the rest service response into one
 or many events. Default: "com.stratio.ingestion.source.rest.handler.DefaultRestSourceHandler".
- jsonPath: field path from which we want recovery the response data. Default: "".
- urlHandler: UrlHandler implementation classname. Currently you have two implementations:
     - DefaultUrlHandler: return url
     - DynamicUrlHandler: check url and replace query params specified as ${param_name}.
- urlHandlerConfig: json file path where you have to specify how to handler query params. e.g. If you have an url such as ***http://localhost:34545/metrics?from_date=${date}***, you have to specify how to parse  ${date} param: 

	```
	{
     	"filterHandler": "com.stratio.ingestion.source.rest.url.filter.MongoFilterHandler",
     	"filterConfiguration" :"../conf/filterConfig.json",
     	"urlParamMapper" : "{\"params\":[{\"name\":\"date\", \"default\":\"1970-01-01%2000:00:00\"}]}"
   	}
	```
   Currently, there is only one implementation for filterHandler: MongoFilterHandler. In this case, we recovery "date" info from MongoDB dynamically. In order to do that, we specify withtin filterConfig.json file how to access to MongoDB instance and how to parse the field:
	``` 
	{
  		"field": "date",
  		"type": "com.stratio.ingestion.source.rest.url.filter.type.DateCheckpointType",
  		"dateFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
  		"mongoUri": "mongodb://socialLoginUser:temporal@180.205.132.228:50017,180.205.132.229:50017,180.205.132.230:50017/socialLogin.checkpoints?replicaset=socialLogin&ssl=true"
	}
	```
This source will send a request to url target every 'frequency' seconds, retrieve the response and put it into its flume channel.

Sample Flume config
-------------------

The following file describes an example configuration of a flume agent that request metrics from flume web server and log them using a memory channel.

```
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
```

Building Stratio REST Source
-------------------------------

The source is built using Maven:

mvn clean package

Run example
-------------------------------

Copy .jar resuling from package and run agent on flume

bin/flume-ng agent --conf conf/ -f agent.conf -n agent -Dflume.monitoring.type=http -Dflume.monitoring.port=34545