Stratio Ingestion
=================

[![Build Status](https://travis-ci.org/Stratio/stratio-ingestion.svg?branch=develop)](https://travis-ci.org/Stratio/stratio-ingestion)

Stratio Ingestion is a very easy-to-use ETL engine which allows the user to define its own workflows through a gui application.

Stratio Ingestion consists of the following components:

* Data transporter and collector: [Apache Flume](http://flume.apache.org/)
* Data extractor and transformer: [Morphlines](http://kitesdk.org/docs/current/kite-morphlines/index.html)
* Custom sinks to write the data to:
    - Cassandra
    - MongoDB
    - [Stratio Streaming](https://github.com/Stratio/stratio-streaming)
    - JDBC
* Visual editor that makes the ETL flow design a child's play (TODO).

