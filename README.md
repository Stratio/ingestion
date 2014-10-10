Stratio Ingestion
=================

[![Build Status](https://travis-ci.org/Stratio/stratio-ingestion.svg?branch=develop)](https://travis-ci.org/Stratio/stratio-ingestion)


FLUME-INGESTION
===================


Flume Ingestion is a fork of Apache Flume (1.6), where you can find:

**Several bug fixes**

 - Some of them really important, such as unicode support

**Several enhancements of Flume's sources & sinks**

 - ElasticSearch mapper, for example

**Custom sources and sinks, developed by Stratio**

 - SNMP (v1, v2c and 3)
 - redis, Kafka (0.8.1.1)
 - MongoDB, JDBC and Cassandra
 - Stratio Streaming (Complex Event Processing engine)
 - REST client, Flume agents stats
 


Flume Ingestion components
----------------------------


* Data transporter and collector: [Apache Flume](http://flume.apache.org/)
* Data extractor and transformer: [Morphlines](http://kitesdk.org/docs/current/kite-morphlines/index.html)
* Custom sources to read data from:
    - REST
    - FlumeStats
    - SNMPTraps
* Custom sinks to write the data to:
    - Cassandra
    - MongoDB
    - [Stratio Streaming](https://github.com/Stratio/stratio-streaming)
    - JDBC
    - Kafka
* Visual editor that makes the ETL flow design a child's play (TODO).


What is Apache Flume?
--------------------------

Apache Flume is a distributed, reliable, and available system for efficiently collecting, aggregating and moving large amounts of log data from many different sources to a centralized data store.

Its use is not only designed for logs, in fact you can find a myriad of sources, sinks and transformations.

In addition, a sink could be a big data storage but also another real-time system (Apache Kafka, Spark Streaming).


Interesting facts about Flume-Ingestion
-----------------------------------------------

 * Flume Ingestion is Apache Flume "on steroids" :)
 
 * We are extensively using Kite SDK (morphlines) in order to do a better T from ETL, and so we have also developed a bunch of custom transformations.
 
 * Stratio ingestion is fully open source and we work very close to the Flume community.

Flume Ingestion FAQ
-------------------------


**Can I use Flume Ingestion for aggregating data (time-based rollups, for example)?**

*This is not a good idea from our experience, we use to combine Flume + Spark Streaming in order to do that (custom development)*

**Is Flume Ingestion multipersistence?**

*Yes, you can write data to JDBC sources, mongoDB, Apache Cassandra, ElasticSearch, Apache Kafka, among others.*


**Can I send data to streaming-cep-engine?**

*Of course, we have developed a sink in order to send events from Flume to an existing stream in our CEP engine.  The sink will create the stream if it does not exist in the engine.*




