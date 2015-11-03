Quick Reference
***************

Here you can find a quick reference of existing elements, ready to use in your Ingestion deployment:

Sources
=======

*   Avro (Data serialization/RPC)
*   Thrift (Data serialization/RPC)
*   Exec (unix commands output)
*   JMS (queue system)
*   Spooling (watch for files changed in a folder)
*   Twitter Firehose 1% (experimental)
*   Kafka (queue system)
*   Netcat (data from a specified ip:port)
*   Sequence generator (generation of counter)
*   Syslog (data from syslog, TCP or UDP)
*   HTTP (data from HTTP POST or GET calls)
*   Stress (load generation for testing)
*   Scribe (adapter for Scribe ingestion system)
*   REST (calls to a REST service)
*   Redis (data from Redis Pub/Sub system).
*   IRC (IRC chat protocol)
*   SNMP traps (SNMP asynchronous notifications)

Transformations
===============

*   Flume Interceptors: https://flume.apache.org/FlumeUserGuide.html#flume-interceptors
*   Morphline Interceptors: http://www.cloudera.com/content/www/en-us/documentation/archive/search/1-3-0/Cloudera-Search-User-Guide/csug_flume_morphline_interceptor_config_options.html
*   Stratio Custom Interceptors: https://github.com/Stratio/Morphlines


Channels
========

*   Memory channel
*   JDBC channel
*   Kafka channel
*   File channel
*   Spillable Memory channel
*   Pseudo Transaction channel

Sinks
=====

*   HDFS
*   Hive
*   Logger
*   Avro
*   Thrift
*   IRC
*   File Roll
*   Null
*   HBase
*   MorphlineSolr
*   ElasticSearch
*   Kite Dataset
*   Kafka
*   Custom
*   Cassandra
*   Druid
*   JDBC
*   Kafka
*   MongoDB
*   Stratio Decision


List of API's
-------------

-   com.stratio.decision
    -   API     custom api in Scala for Stratio Decision
-   com.stratio.decision
    -   Siddhi  Siddhi CEP is a lightweight, easy-to-use Open Source Complex Event Processing Engine (CEP) under Apache Software License v2.0. Siddhi CEP processes events which are triggered by various event sources and notifies appropriate complex events according to the user specified queries.
-   com.datastax.cassandra
    -   cassandra-driver-core   driver of Datastax to connect to Cassandra database
-   org.apache.kafka
    -   kafka_2.10  driver of Apache to connect to Kafka
-   org.mongodb
    -   mongo-java-driver   driver of MongoDB to connect java with a mongoDB database
-   org.apache.solr
    -   solr-solrj  Java client to access solr. It offers a java interface to add, update, and query the solr index.
-   com.codahale.metrics
    -   metrics-core    A Go library which provides light-weight instrumentation for your application.
-   com.ryantenney.metrics
    -   metrics-spring  The metrics-spring module integrates Dropwizard Metrics library with Spring, and provides XML and Java configuration.
-   org.mockito
    -   mockito-all Mockito is a mocking framework for unit tests in Java and has an automated release system
-   junit
    -   junit 4.10  JUnit is a simple framework to write repeatable tests. It is an instance of the xUnit architecture for unit testing frameworks.
