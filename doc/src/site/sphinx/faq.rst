Flume Ingestion FAQ
===================

**Can I use Flume Ingestion for aggregating data (time-based rollups, for example)?**

*This is not a good idea from our experience, we use to combine Flume + Spark Streaming in order to do that (custom development)*

**Is Flume Ingestion multipersistence?**

*Yes, you can write data to JDBC sources, mongoDB, Apache Cassandra, ElasticSearch, Apache Kafka, among others.*

**Can I send data to decision-cep-engine?**

*Of course, we have developed a sink in order to send events from Flume to an existing stream in our CEP engine. The sink will create the stream if it does not exist in the engine.*

.. _Apache Flume: http://flume.apache.org/
.. _Morphlines: http://kitesdk.org/docs/current/kite-morphlines/index.html
.. _Stratio Decision: https://github.com/Stratio/Decision

