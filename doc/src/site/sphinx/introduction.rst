Introduction
************

What is Apache Flume?
=====================

Apache Flume is a distributed, reliable, and available system for efficiently collecting, aggregating and moving large amounts of log data from many different sources to a centralized data store.

Its use is not only designed for logs, in fact you can find a myriad of sources, sinks and transformations.

In addition, a sink could be a big data storage but also another real-time system (Apache Kafka, Spark Streaming).


Interesting facts about Flume-Ingestion
=======================================

-  Flume Ingestion is Apache Flume “on steroids” :)

-  We are extensively using Kite SDK (morphlines) in order to do a better T from ETL, and so we have also developed a bunch of custom transformations.

-  Stratio Ingestion is fully open source and we work very close to the Flume community.


Ingestion components
==========================

-  Data transporter and collector: `Apache Flume`_
-  Data extractor and transformer: `Morphlines`_
-  Custom sources to read data from:

   -  REST
   -  FlumeStats
   -  SNMPTraps
   -  IRC

-  Custom sinks to write the data to:

   -  Cassandra
   -  MongoDB
   -  Druid
   -  `Stratio Decision`_
   -  JDBC
   -  Kafka

