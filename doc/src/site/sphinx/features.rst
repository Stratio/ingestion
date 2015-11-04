Features Guide
**************

Flume Ingestion is a fork of Apache Flume (1.6), where you can find some additional features:


**Custom sources and sinks, developed by Stratio**

-   Sources

    -  REST:    A Flume source that make a request to a REST service.

    -  Redis:   A Flume source that read data from Redis Pub Sub system with connection Pool. Accept patterns.

    -  IRC:     A Stratio Ingestion source to get data from IRC.

    -  SNMP traps:  A Flume source that listens to snmp traps.



-   Sinks

    -   Cassandra:  The Cassandra Sink component allows to save Flume-flow events into Cassandra.It tries to find the defined fields in the event's headers. If a "data" field is defined, it will take the body's event instead of a header.

    -   MongoDB: The MongoDB Sink component allows to save Flume-flow events to MongoDB. It can parse both event body and headers.

    -   Druid: The Druid Sink component allows to save Flume-flow events to Druid.

    -   JDBC: Stratio JDBC Sink saves Flume events to any database with a JDBC driver.It can operate either with automatic headers-to-tables mapping or with custom SQL queries.

    -   Kafka:  Kafka Sink.  Send events to Kafka.

    -   Stratio Decision (Complex Event Processing engine) :    A Flume sink using Stratio Decision.The Stratio Decision Sink will insert flume events to an specific stream. The configuration is located in the flume config

    -   Data transporter and collector: [Apache Flume](http://flume.apache.org/)

    -   Data extractor and transformer: [Morphlines](http://kitesdk.org/docs/current/kite-morphlines/index.html)


