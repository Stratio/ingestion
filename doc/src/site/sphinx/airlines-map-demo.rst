US flights Demo
***************

This demo downloads and ingests part of the `Airline on-time performance dataset`_ to ElasticSearch.

.. _Airline on-time performance dataset: http://stat-computing.org/dataexpo/2009/the-data.html

Preparing the environment
=========================

Manually
--------

You need an extracted full distribution of Stratio Ingestion at ``/opt/sds/ingestion``. You can use a different path via the ``INGESTION_HOME`` environment variable. You will also need a running ElasticSearch.

Vagrant Box
-----------

You can use our Vagrant Box to run the example, just type: ``vagrant up stratio/ingestion`` to get our sandbox.

You can edit the conf/flume-conf.properties for customizing the example. By default, we have activated two sinks: ElasticSearch and Cassandra, but we provide the configuration for Stratio Streaming Sink commented in the same file.


Running the example
===================

ElasticSearch
-------------

- Edit ``conf/flume-conf-elasticsearch.properties`` to set up your ElasticSearch cluster hostnames (``a.sinks.snk.hostNames``) and cluster name (``a.sinks.snk.clusterName``). By default, they are ``127.0.0.1`` and ``elasticsearch`` respectively.
- Run ``bin/run_example.sh``.

Kibana Dashboard
----------------

- Start Kibana if it is stopped (``./opt/sds/kibana-4.0.2-linux-x64/kibana``) Enter in Kibana http://IP:5601 in Dashboard Tab You can Load saved dashboard and select "US Flights Demo Dashboard". Due to date of logs, you must select a new time filter between January 1987 and December 1988, for example. You should see something like this:

 .. image:: /images/flume-airlines.jpg
    :align: center

