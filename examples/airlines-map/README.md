
US flights Demo
=========================

This demo downloads and ingests part of the [Airline on-time performance dataset](http://stat-computing.org/dataexpo/2009/the-data.html) to ElasticSearch.

Preparing the environment
-------------------------

### Manually

You need an extracted full distribution of Stratio Ingestion at `/opt/sds/ingestion`. You can use a different path via the `INGESTION_HOME` environment variable. You will also need a running ElasticSearch.

Running the example
-------------------

### ElasticSearch


- Edit `conf/flume-conf-elasticsearch.properties` to set up your ElasticSearch cluster hostnames (`a.sinks.snk.hostNames`) and cluster name (`a.sinks.snk.clusterName`). By default, they are `127.0.0.1` and `elasticsearch` respectively.
- Run `bin/run_example.sh`.

### Kibana Dashboard

We provide an out-of-the-box configured Kibana dashboard for visualizing the ingested data. You'll need an installation of [Kibana 4](https://www.elastic.co/downloads/kibana), [elasticdump] (https://github.com/taskrabbit/elasticsearch-dump/) and a version of ElasticSearch equals or later than 1.4.4.

Load the provided dashboard inside ElasticSearch with the commands `elasticdump --input $INGESTION_HOME/examples/airlines-map/dashboards/us-flights-mapping.json --output=http://[YOUR ES HOST]:[YOUR ES PORT]/.kibana --type=mapping` and `elasticdump --input $INGESTION_HOME/examples/airlines-map/dashboards/us-flights-data.json --output=http://[YOUR ES HOST]:[YOUR ES PORT]/.kibana --type=data`.
 
Now, you will be able to access the preconfigured Kibana 4 dashboard through an URL like [this](http://localhost:5601/#/dashboard/US-Flights?_g=(refreshInterval:(display:Off,section:0,value:0),time:(from:'1987-12-31T23:00:00.000Z',mode:absolute,to:'1988-12-31T23:00:00.000Z'))&_a=(filters:!(),panels:!((col:1,id:Flights-by-Origin,row:1,size_x:6,size_y:6,type:visualization),(col:7,id:Flights-by-Destination,row:1,size_x:6,size_y:6,type:visualization),(col:1,id:Average-Arrival-Delay-per-day,row:7,size_x:6,size_y:4,type:visualization),(col:7,id:Average-Departure-Delay-per-day,row:7,size_x:6,size_y:4,type:visualization),(col:1,id:Top-20-Arrival-Delay-by-Unique-Carrier,row:11,size_x:3,size_y:2,type:visualization),(col:4,id:Top-20-Departure-Delay-by-Unique-Carrier,row:11,size_x:3,size_y:2,type:visualization),(col:7,id:Top-20-Average-Departure-Delay-By-Destination-Airport,row:11,size_x:3,size_y:2,type:visualization),(col:10,id:Top-20-Average-Arrival-Delay-by-Origin-Airport,row:11,size_x:3,size_y:2,type:visualization)),query:(query_string:(analyze_wildcard:!t,query:'*')),title:US-Flights).


