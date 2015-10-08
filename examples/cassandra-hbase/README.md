Cassandra & Hbase example
=====================

Agents configuration
--------------------

In this example we will start an agent which will be listening on a specific port Avro requests receiving 
purchase orders. The orders will be recived in JSON format. The agent will process the order requests, using the 
giving information will enrich the input data and will clean some fields. After this process the agent will send the 
order requests to Cassandra and Hbase. 

To run the agents you need to execute the bin/run_flume.sh script.

The full agent configuration is the following (see flume-conf.properties):

* Source: 
  - Avro

* Channels:
  - 2 memory channels for cassandra and hbase

* Sinks:
  - Cassandra sink (developed by Stratio: https://github.com/Stratio/Ingestion/tree/master/stratio-sinks/stratio-cassandra-sink)
  - HBase Async sink
  

Preparing the environment
-------------------------

You can edit the conf/flume-conf.properties for customizing the example. 


Running the example
-------------------

To run the agents (in deattached mode) just type:

```
screen -S ingestion -t ingestion -m bin/run_flume.sh
```

After start the agent you can run the orders generator (https://github.com/Stratio/fakenator/tree/feature/INGESTION-29_hbase_demo):
```
java  -jar fakenator-1.0-SNAPSHOT-jar-with-dependencies.jar -r 3 -t 1000
```


In local environments you can use docker instances of Cassandra and Hbase to run the example. Example to run Hbase 
using docker:
$ docker run -dit --name hbase --hostname hbase.hbase-standalone.local.dev -p 2181:2181 -p 60000:60000 -p 60010:60010 -p 60020:60020 -p 60030:60030 banno/hbase-standalone

*Hbase Support Commands*

hbase(main):007:0* create 'orders','cf'
hbase(main):008:0> list
hbase(main):015:0> scan 'orders'


