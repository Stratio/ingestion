Features Guide
**************

Stratio Ingestion is a fork of Apache Flume (1.6), where you can find some additional features:


**Custom sources and sinks, developed by Stratio**

-   Sources (From where we can read in addition to Apache Flume):

    -  `REST`_ A Flume source that make a request to a REST service.

    -  `Redis`_ A Flume source that read data from Redis Pub Sub system with connection Pool. Accept patterns.

    -  `IRC`_ A Stratio Ingestion source to get data from IRC.

    -  `SNMP Traps`_ A Flume source that listens to snmp traps.



-   Sinks (Where we can store the information in addition to Apache Flume):

    -   `Cassandra`_  The Cassandra Sink component allows to save Flume-flow events into Cassandra.It tries to find the defined fields in the event's headers. If a "data" field is defined, it will take the body's event instead of a header.

    -   `MongoDB`_ The MongoDB Sink component allows to save Flume-flow events to MongoDB. It can parse both event body and headers.

    -   `Druid`_ The Druid Sink component allows to save Flume-flow events to Druid.

    -   `JDBC`_ Stratio JDBC Sink saves Flume events to any database with a JDBC driver.It can operate either with automatic headers-to-tables mapping or with custom SQL queries.

    -   `Kafka`_  Kafka Sink.  Send events to Kafka.

    -   `Stratio Decision`_(Complex Event Processing engine) :    A Flume sink using Stratio Decision.The Stratio Decision Sink will insert flume events to an specific stream. The configuration is located in the flume config

-   Transformations:

    -   Flume Transformations: [Apache Flume](http://flume.apache.org/)

    -   Stratio Custom Transformations: [Stratio Morphlines](http://docs.stratio.com/modules/morphlines/development/)

    -   Morphlines Transformations using Kite SDK: [Morphlines](http://kitesdk.org/docs/current/morphlines/)


.. _REST: https://github.com/Stratio/Ingestion/tree/master/stratio-sources/stratio-rest-source
.. _Redis: https://github.com/Stratio/Ingestion/tree/master/stratio-sources/stratio-redis-source
.. _IRC: https://github.com/Stratio/Ingestion/tree/master/stratio-sources/stratio-irc-source
.. _REST: https://github.com/Stratio/Ingestion/tree/master/stratio-sources/stratio-rest-source
.. _SNMP Traps: https://github.com/Stratio/Ingestion/tree/master/stratio-sources/stratio-snmptraps-source
.. _Cassandra: https://github.com/Stratio/Ingestion/tree/master/stratio-sinks/stratio-cassandra-sink
.. _MongoDB: https://github.com/Stratio/Ingestion/tree/master/stratio-sinks/stratio-mongodb-sink
.. _Druid: https://github.com/Stratio/Ingestion/tree/master/stratio-sinks/stratio-druid-sink
.. _JDBC: https://github.com/Stratio/Ingestion/tree/master/stratio-sinks/stratio-jdbc-sink
.. _Kafka: https://github.com/Stratio/Ingestion/tree/master/stratio-sinks/stratio-kafka-sink
.. _Stratio Decision: https://github.com/Stratio/Ingestion/tree/master/stratio-sinks/stratio-decision-sink