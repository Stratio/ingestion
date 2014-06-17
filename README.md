Stratio Ingestion
======================

Stratio Ingestion is a very easy-to-use ETL engine which allows the user to define its own workflows through a gui application.

See following the Stratio Ingestion components:

* Data transporter and collector: [Apache flume](http://flume.apache.org/)
* Data extractor and transformer: [Morphlines](http://kitesdk.org/docs/current/kite-morphlines/index.html)
* Customized sinks to write the data to:
    - Cassandra
    - MongoDB
    - Stratio streaming
* GUI application which makes the ETL flow design a child's play.