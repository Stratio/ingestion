Getting Started
***************

How do I get started?
=====================

-   Prerequisites

    Stratio Ingestion works in any UNIX server with Java (>= 1.7) installed. If you want to read or store data from remote places, you need also connectivity with those remote services (databases and so on).

    -   System Requirements

        Depending of the topology of your system you can configure Ingestion properties files in order to adapt it to your project.

    -   Software/Other module Requirements

        No needed additional requirements a part of Java and network conectivity yo the remote services.

    -   Skills Needed

        As a developer you only need a basic knowledge about Flume's components and how to modify configuration files
        to build it properly.


-   Download

    Download the code from the Stratio Ingestion repository: https://github.com/Stratio/Ingestion/

-   Build and Install

    After clone go to the project folder. Once you do it you update de project with git submodules, and compile with
    maven to be able to install.

::
    $ git clone https://github.com/Stratio/Ingestion/
    $ cd Ingestion
    $ git submodule init
    $ git submodule update
    $ mvn install
    $ cd stratio-ingestion-dist
    $ mvn clean compile package

Distribution will be available at ``stratio-ingestion-dist/target`` folder and you can find the tar.gz file .deb
 and .rpm packages to install.

-   Configuration

    Each Flume-Ingestion uses different files to configure and execute the agent. You can take a look at existing
    examples to see some possible configurations.

-   Properties files. Usually in the conf directory (Example: /home/userName/airlines-map/conf/flume-conf.properties) you can find the agent
    configuration files. The file with the workflow configuration has a .properties extension and includes the
    definition, configuration & mapping of different Ingestion components (Sources, Transformations, Channels and
    Sinks).

::

    a.sources=avroSource
    a.channels=cassandraChan hBaseChan
    a.sinks=cassandraSink hBaseSink

    #### SOURCES ######
    a.sources.avroSource.interceptors = morphlineinterceptor
    a.sources.avroSource.interceptors.morphlineinterceptor.type = org.apache.flume.sink.solr.morphline.MorphlineInterceptor$Builder
    a.sources.avroSource.interceptors.morphlineinterceptor.morphlineFile = conf/interceptor.conf
    a.sources.avroSource.interceptors.morphlineinterceptor.morphlineId = morphline1

    a.sources.avroSource.type = avro
    a.sources.avroSource.bind = 0.0.0.0
    a.sources.avroSource.port = 4141

    ###### CHANNELS ######
    a.channels.cassandraChan.type=memory
    a.channels.cassandraChan.capacity = 20000
    a.channels.cassandraChan.transactionCapacity = 100

    ###### SINKS ######
    a.sinks.cassandraSink.type=com.stratio.ingestion.sink.cassandra.CassandraSink
    a.sinks.cassandraSink.tables=orders.orders
    a.sinks.cassandraSink.batchSize=100
    a.sinks.cassandraSink.hosts=cs.cassandra.local.dev:9042
    a.sinks.cassandraSink.cqlFile=conf/init-cassandra-orders.cql

    ####### MAPPING #####
    a.sources.avroSource.channels=cassandraChan
    a.sinks.cassandraSink.channel=cassandraChan


Each different component have specific configuration so if you need to configure any flume component you can check
the flume reference guide (https://flume.apache.org/FlumeUserGuide.html) or if is about any specific Ingestion
component you can check http://docs.stratio.com/modules/ingestion/development/configuration.html .

-   Run Stratio Ingestion

To run a specific agent, usually in the bin directory you can find a shell script with name run_flume.sh which start
the agent. In this shell script you can pass the needed parameters by agent. Example:

::

    #!/bin/bash
    INGESTION_HOME="${INGESTION_HOME:-/opt/sds/ingestion}"
    cd "$(dirname $0)/../"
    exec "${INGESTION_HOME}/bin/flume-ng" agent --conf ./conf --conf-file ./conf/flume-conf.properties --name a -Dflume.monitoring.type=http -Dflume.monitoring.port=34545








Examples
--------

In our examples you can see some correct configurations with all Flume's components configured and connected.


-   US Flights demo: https://github.com/Stratio/Ingestion/tree/master/examples/airlines-map

-   NASA Apache Logs demo: https://github.com/Stratio/Ingestion/tree/master/examples/apache-logs

-   Cassandra & HBase example: https://github.com/Stratio/Ingestion/tree/master/examples/cassandra-hbase

-   Wikipedia Pagecounts demo: https://github.com/Stratio/Ingestion/tree/master/examples/wikipedia-pagecounts
