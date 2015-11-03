Features Guide
**************

Flume Ingestion is a fork of Apache Flume (1.6), where you can find some additional features:


**Custom sources and sinks, developed by Stratio**

-   Sources

    -  REST (calls to a REST service)

    -  Redis (data from Redis Pub/Sub system)

    -  IRC (IRC chat protocol)

    -  SNMP traps (SNMP asynchronous notifications)



-   Sinks

    -   Cassandra   com.stratio.ingestion.sink.cassandra.CassandraSink

    -   MongoDB com.stratio.ingestion.sink.mongodb.MongoSink

    -   Druid   com.stratio.ingestion.sink.druid.DruidSink

    -   JDBC    com.stratio.ingestion.sink.jdbc.JDBCsink

    -   Kafka   com.stratio.ingestion.sink.kafka.KafkaSink

    -   Stratio Decision (Complex Event Processing engine)

    -   Data transporter and collector: [Apache Flume](http://flume.apache.org/)

    -   Data extractor and transformer: [Morphlines](http://kitesdk.org/docs/current/kite-morphlines/index.html)


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

*This is not a good idea from our experience, but you can use [Stratio Sparkta](https://github.com/Stratio/Sparkta) for real-time aggregation.

**Is Flume Ingestion multipersistence?**

*Yes, you can write data to JDBC sources, mongoDB, Apache Cassandra, ElasticSearch, Apache Kafka, among others.*


**Can I send data to decision-cep-engine?**

*Of course, we have developed a sink in order to send events from Flume to an existing stream in our CEP engine.  The sink will create the stream if it does not exist in the engine.*
