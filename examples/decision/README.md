Decision integration example with data Sharding
===============================================

Agents configuration
--------------------

In this example we will start an agent which will read a file using a SpoolDir source. Associated to this source we 
have configured a channel selector with the following logic:

* If in the "City" header the value is "Madrid" the event will be sent to the Madrid Channel. The Madrid Channel is 
connected to Madrid Decision Sink. This Sink will send the events to the madrid specific topic: 
"stratio_decision_data_madrid"

* If in the "City" header the value is "Barcelona" the event will be sent to the Catalunya Channel. The Catalunya 
Channel is connected to Catalunya Decision Sink. This Sink will send the events to the catalunya specific topic: 
  "stratio_decision_data_catalunya"

* In other case the event will be sent to Spain Channel. The Spain Channel is connected to Spain Decision Sink. This 
sink will check if the event includes de "_topic" header, it has will send the data to this topic, in other case will
 send the data to the spain specific topic: "stratio_decision_data_spain"


To run the agents you need to execute the bin/run_flume.sh script.

The full agent configuration is the following (see flume-conf.properties):

* Source: 
  - SpoolDir

* Channels:
  - 3 memory channels

* Sinks:
  - 3 Decision sinks (developed by Stratio: https://github
  .com/Stratio/Ingestion/tree/master/stratio-sinks/stratio-decision-sink)  
  

Preparing the environment
-------------------------

You can edit the conf/flume-conf.properties for customizing the example. 


Running the example
-------------------

To run the agents (in deattached mode) just type:

```
screen -S ingestion -t ingestion -m bin/run_flume.sh
```

