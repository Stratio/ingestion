Getting Started
***************

How do I get started?
=====================

To explore and play with Stratio Ingestion, we recommend to visit the following:

-   Download the code from the Stratio Ingestion repository: https://github.com/Stratio/Ingestion/

-   :ref:`wikipedia-pagecounts-demo`: Execute one of the existing demos to understand better how to use Ingestion agent

-   :ref:`configuration`: Check some configuration details to understand how to setup your first Ingestion agent

-   Configure your first example agent to get data from any source, transform the data and send to your datastore. Also you can take a look at :ref:`apache-logs-demo` as reference. It could be a good starting point to setup your custom agent


Compile & Package
=================

::

    $ git submodule init
    $ git submodule update
    $ mvn install
    $ cd stratio-ingestion-dist
    $ mvn clean compile package

Distribution will be available at ``stratio-ingestion-dist/target/stratio-ingestion-0.4.0-SNAPSHOT-bin.tar.gz``


System Requirements
===================

Depending of the topology of your system you can configure Flume configuration files in order to adapt it to your
project

Stratio Ingestion only needs a Zookeeper instance and a Kafka instance installed to work properly.

As a developer you only need a basic knowledge about Flume's components and how to modify configuration files to
build it properly.