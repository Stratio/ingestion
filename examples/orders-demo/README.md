Orders Demo
=====================

Agents configuration
--------------------

In this demo we will integrate two agents. The first of them (agent1) will be in charge of read from Avro multiple purchase orders in JSON format (you can find an example in data/tmp/*.json). This agent will clean & enrich the given order and store the order in different places (Elastic Search, Cassandra, HDFS, Stratio Streaming and Avro).
The second agent (agent2) will get the avro output from the previous agent and using a JsonPath deserializer extract the products and generate multiple events, each of them with the product and order information. The results will be saved in Cassandra and Stratio Streaming.

To run the agents you need to execute the bin/run_flume.sh script.

The full agent1 configuration is the following (see flume-agent1.properties):

* Source: 
  - Avro

* Channels:
  - 6 memory channels for avro, cassandra, elastic, file, hdfs and stratio streaming

* Sinks:
  - Cassandra sink (developed by Stratio: https://github.com/Stratio/flume-ng-cassandra-sink, see also the definition_access_log.json attached)
  - Avro sink
  - Elastic sink
  - File roll sink
  - HDFS sink
  - Stratio sink (developed by Stratio: https://github.com/Stratio/flume-ng-stratiostreaming-sink)

  
The full agent12configuration is the following (see flume-agent2.properties):

* Source: 
  - Avro

* Channels:
  - 3 memory channels for cassandra, hdfs and stratio streaming

* Sinks:
  - Cassandra sink (developed by Stratio: https://github.com/Stratio/flume-ng-cassandra-sink, see also the definition_access_log.json attached)  
  - Stratio sink (developed by Stratio: https://github.com/Stratio/flume-ng-stratiostreaming-sink)
  - HDFS sink

  

Preparing the environment
-------------------------

You can edit the conf/flume-agent*.properties for customizing the example. 


Running the example
-------------------

To run the agents (in deattached mode) just type:

```
screen -S ingestion2 -t ingestion2 -m bin/run_flume-2.sh

screen -S ingestion1 -t ingestion1 -m bin/run_flume.sh

```

The agent1 send to output to the Avro sink that will connect with the agent2 so it's better start the agent2 before than agent1.

TODO
-------------------

* The fisrt agent includes a lot of Java code in interceptor.conf file. This interceptor can be refactored to implement some of the business logic using Kite SDK.
There is an initial version of this refactor in interceptor-3.conf file

* The second agent to split the lines in multiple Events use a custom Json interceptor. It would be interesting define some requirements about it and face a custom Json interceptor allowing receive the xpath queries to split the Json in multiple events.
