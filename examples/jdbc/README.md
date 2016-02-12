JDBC Examples (Mysql & Postgres)
================================

Agents configuration
--------------------

In this example we will start an agent which will read a file using a SpoolDir source. Associated to this source we 
have associated 2 channels and 2 JDBC sinks. One of them will save the information in Mysql and the other one in 
Postgresql

To run the agents you need to execute the bin/run_flume.sh script.

The full agent configuration is the following (see flume-conf.properties):

* Source: 
  - SpoolDir

* Channels:
  - 2 memory channels

* Sinks:
  - 2 JDBC Sinks (developed by Stratio: https://github.com/Stratio/Ingestion/tree/master/stratio-sinks/stratio-jdbc-sink)  
  

Preparing the environment
-------------------------

Steps:
* Edit the conf/flume-conf.properties for customizing the example to your environment. Pay attention to hosts, users 
and passwords. 

* Postgresql and Mysql drivers are not provided. So to run this example you need to edit the bin/flume-env.sh file and 
set in the FLUME_CLASSPATH variable the full path to the Postgresql and Mysql drivers.

* Create the cities table in Mysql and Postgres using the following Sql sentence:
    - CREATE TABLE cities (id INT, name VARCHAR(100), city VARCHAR(100));


Running the example
-------------------

To run the agents (in deattached mode) just type:

```
screen -S ingestion -t ingestion -m bin/run_flume.sh
```