Get Started example
===================

Agents configuration
--------------------

In this example we will start an agent which will read from network some data and log it to sysout. To run the agents you need to execute the bin/run_flume.sh script.

The full agent configuration is the following (see flume-conf.properties):

* Source: 
  - Netcat

* Channels:
  - 1 memory channels

* Sinks:
  - 1 logger sink
  

Preparing the environment
-------------------------

You can edit the conf/flume-conf.properties for customizing the example. 


Running the example
-------------------

To run the agents (in deattached mode) just type:

```
screen -S ingestion -t ingestion -m bin/run_flume.sh
```

