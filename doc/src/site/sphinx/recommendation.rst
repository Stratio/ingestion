Best Practices and Recommendation Configuration
===============================================

To explain some tips to configure a correct properties we will use the .properties from previous sections:

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

For a correct build of the properties there are few fields required to be filled or the build will fail:

    - Names of the components:

        a.sources=avroSource
        a.channels=cassandraChan hBaseChan
        a.sinks=cassandraSink hBaseSink

    - When declaring a component it's important to fill the .type property and any of his required fields:

        a.sources.avroSource.type = avro
        a.channels.cassandraChan.type=memory
        a.sinks.cassandraSink.type=com.stratio.ingestion.sink.cassandra.CassandraSink

    - Be sure to bind all the components (a sink with a channel and a channel to a source) in last section when you
    have declared previously all the components:

        ####### MAPPING #####

        a.sources.avroSource.channels=cassandraChan
        a.sinks.cassandraSink.channel=cassandraChan