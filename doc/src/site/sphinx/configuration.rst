Ingestion Configuration
*********************

Compile & Package
=================

::

    $ git submodule init
    $ git submodule update
    $ mvn install
    $ cd stratio-ingestion-dist
    $ mvn clean compile package

Distribution will be available at ``stratio-ingestion-dist/target/stratio-ingestion-0.4.0-SNAPSHOT-bin.tar.gz``


Agents Configuration
====================

Each Flume-Ingestion uses different files to configure and execute the agent. You can take a look at existing
examples to see some possible configurations:

-   Properties files. Usually in the conf directory you can find the agent configuration files. The file with the
workflow configuration has a .properties extension and includes the definition, configuration & mapping of different
Ingestion components (Sources, Transformations, Channels and Sinks).

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
component you can check the README.md file of the proper component in the repository .

To run a specific agent, usually in the bin directory you can find a shell script with name run_flume.sh which start
the agent. In this shell script you can pass the needed parameters by agent. Example:

::

    #!/bin/bash
    INGESTION_HOME="${INGESTION_HOME:-/opt/sds/ingestion}"
    cd "$(dirname $0)/../"
    exec "${INGESTION_HOME}/bin/flume-ng" agent --conf ./conf --conf-file ./conf/flume-conf.properties --name a -Dflume.monitoring.type=http -Dflume.monitoring.port=34545




Trouble Shooting Guide
======================

The most important points to pay attention in Ingestion agents configurations are:

-   Connectivity problems with remote systems. If you're getting or writing data in an external platform double
check you don't have a connectivity problem before to run your agents.

-   Problems with transformations. If you're creating your custom transformations it's a common problem that if any
morphline command fails, you don't see any data in your sinks. Go step by step adding your morphlines commands
checking that are producing the expected results. Also is useful to use the logDebug, logWarn, logInfo commands to
print the results after a morphline transformation:
::


    { logDebug { format : "Test: {}", args : ["@{}"] } }

-   Sources, Channels and Sinks are configured properly. Check that sources have associated the required channels,
and check also that sinks are associated with channels.

-   Some sinks write data that comes in event headers, other sinks use information of event body. So oi you're
sure that are transforming the data properly and you're not seeing results in your datastore, review the sink
configuration to check if this Sink require information in headers or body.

