
Wikipedia Pagecounts Demo
=========================

This demo downloads and ingests the [Page view statistics for Wikimedia projects](https://dumps.wikimedia.org/other/pagecounts-raw/) (Wikipedia Pagecounts) to ElasticSearch, MongoDB or Cassandra.

Requirements
------------

You need an extracted full distribution of Stratio Ingestion at `/opt/sds/ingestion`. You can use a different path via the `INGESTION_HOME` environment variable. You will also need a running ElasticSearch, MongoDB or Cassandra. By default, it will use only ElasticSearch. See below for different set ups you can use.

ElasticSearch
-------------

- Edit `conf/flume-conf-elasticsearch.properties` to set up your ElasticSearch cluster hostnames (`a.sinks.snk.hostNames`) and cluster name (`a.sinks.snk.clusterName`). By default, they are `127.0.0.1` and `elasticsearch` respectively.
- Run `DB=elasticsearch bin/run_example.bash`.

MongoDB
-------

- Edit `conf/flume-conf-mongodb.properties` to set up your MongoDB URI (`a.sinks.snk.mongoUri`). By default, it is `mongodb://127.0.0.1:27017/flume_wikipedia_demo.wikipedia_demo`.
- Run `DB=mongodb bin/run_example.bash`.

Cassandra
---------

- Edit `conf/flume-conf-cassandra.properties` to set up your Cassandra host (`a.sinks.snk.host`) and cluster name (`a.sinks.snk.clusterName`). By default, they are `127.0.0.1` and `Test Cluster` respectively.
- Run `DB=cassandra bin/run_example.bash`.


