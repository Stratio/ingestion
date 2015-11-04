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


