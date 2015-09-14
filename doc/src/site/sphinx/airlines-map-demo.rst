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

You can use our Vagrant Box to run the example, just type: ``vagrant init stratio/ingestion`` to get our sandbox, then ``vagrant up`` to start it up.

You can edit the /opt/sds/ingestion/examples/airlines-map/conf/flume-conf.properties for customizing the example. By default, we have activated two sinks: ElasticSearch and Cassandra, but we provide the configuration for Stratio Streaming Sink commented in the same file.

Due to an issue in virtualbox in Windows 10 computers, sandbox could not install properly the network interfaces. There is an official Virtualbox patch it. Just download and run as administrator:
https://www.virtualbox.org/attachment/ticket/14040/VBox-Win10-fix-14040.exe
If you've this problem in your local, please run this VirtualBox fix in the background while executing vagrant up.

Running the example
===================

ElasticSearch
-------------

- Edit ``/opt/sds/ingestion/examples/airlines-map/conf/flume-conf-elasticsearch.properties`` to set up your ElasticSearch cluster hostnames (``a.sinks.snk.hostNames``) and cluster name (``a.sinks.snk.clusterName``). By default, they are ``127.0.0.1`` and ``elasticsearch`` respectively.
- Run ``bin/run_example.sh``.

Kibana Dashboard
----------------

- Start Kibana if it is stopped (``./opt/sds/kibana-4.0.2-linux-x64/kibana``) Enter in Kibana http://IP:5601 in Dashboard Tab You can Load saved dashboard and select "US Flights Demo Dashboard". Due to date of logs, you must select a new time filter between January 1987 and December 1988, for example. You can select that date range by clicking on the top right corner, selecting  the "Absolute" option on the left menu and establishing the dates in the calendar:

.. image:: /images/airlines_map_time.jpg
    :align: center

You should see something like this:

 .. image:: /images/flume-airlines.jpg
    :align: center

