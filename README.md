[![Coverage Status](https://coveralls.io/repos/github/Stratio/Ingestion/badge.svg?branch=master)](https://coveralls.io/github/Stratio/Ingestion?branch=master)

Stratio Ingestion
=================

Contents
--------

* Introduction
* Stratio Ingestion components
* Details about Stratio Ingestion
* Compile & Package
* FAQ


Introduction
------------

Stratio Ingestion started as a fork of Apache Flume (1.6), where you can find:

**Custom sources and sinks, developed by Stratio**

 - SNMP (v1, v2c and 3)
 - redis, Kafka (0.8.1.1)
 - MongoDB, JDBC, Cassandra and Druid
 - Stratio Decision (Complex Event Processing engine)
 - REST client, Flume agents stats
 
**Several bug fixes**

 - Some of them really important, such as unicode support

**Several enhancements of Flume's sources & sinks**

 - ElasticSearch mapper, for example

You can find more documentation about us [here](https://stratio.atlassian.net/wiki/display/PLATFORM/STRATIO+INGESTION)


Stratio Ingestion components
----------------------------

* Data transporter and collector: [Apache Flume](http://flume.apache.org/)
* Data extractor and transformer: [Morphlines](http://kitesdk.org/docs/current/kite-morphlines/index.html)
* Custom sources types to read data from:
    - REST   com.stratio.ingestion.source.rest.RestSource
    - Redis FlumeStats   com.stratio.ingestion.source.redis.RedisSource
    - SNMPTraps   com.stratio.ingestion.source.snmptraps.SNMPSource
    - IRC   com.stratio.ingestion.source.irc.IRCSource
* Custom sinks types to write the data to:
    - Cassandra   com.stratio.ingestion.sink.cassandra.CassandraSink
    - MongoDB   com.stratio.ingestion.sink.mongodb.MongoSink
    - [Stratio Decision](https://github.com/Stratio/Decision)
    - JDBC   com.stratio.ingestion.sink.jdbc.JDBCsink
    - Kafka   com.stratio.ingestion.sink.kafka.KafkaSink
    - Druid   com.stratio.ingestion.sink.druid.DruidSink

Details about Stratio Ingestion
-------------------------------

Stratio Ingestion is based on Apache Flume so the first question is:

**What is Apache Flume?**

Apache Flume is a distributed, reliable, and available system for efficiently collecting, aggregating and moving large amounts of log data from many different sources to a centralized data store.

Its use is not only designed for logs, in fact you can find a myriad of sources, sinks and transformations.

In addition, a sink could be a big data storage but also another real-time system (Apache Kafka, Spark Streaming).


**Interesting facts about Stratio Ingestion**

 * Flume Ingestion is Apache Flume "on steroids" :)
 
 * We are extensively using Kite SDK (morphlines) in order to do a better T from ETL, and so we have also developed a bunch of custom transformations.
 
 * Stratio ingestion is fully open source and we work very close to the Flume community.


Compile & Package
-----------------

```
$ mvn clean compile package -Ppackage
```

Distribution will be available at stratio-ingestion-dist/target/ folder. You will find .deb, .rpm and .tar.gz packages ready to use depending your environment.
If you take a look at [documentation](https://stratio.atlassian.net/wiki/display/PLATFORM/STRATIO+INGESTION) you will find more details about how to install the product, and some useful examples to get a better understanding about Stratio Ingestion. 


FAQ
---


**Can I use Stratio Ingestion for aggregating data (time-based rollups, for example)?**

*This is not a good idea from our experience, but you can use [Stratio Sparkta](https://github.com/Stratio/Sparkta) for real-time aggregation.

**Is Flume Ingestion multipersistence?**

*Yes, you can write data to JDBC sources, mongoDB, Apache Cassandra, ElasticSearch, Apache Kafka, among others.*


**Can I send data to decision-cep-engine?**

*Of course, we have developed a sink in order to send events from Flume to an existing stream in our CEP engine.  The sink will create the stream if it does not exist in the engine.* 

**Where can I find more details about Stratio Ingestion?**

*You can take a look at our Documentation on [Confluence](https://stratio.atlassian.net/wiki/display/PLATFORM/STRATIO+INGESTION)


Changelog
---------

See the [changelog](CHANGELOG.md) for changes. 


