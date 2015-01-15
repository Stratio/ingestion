Stratio Cassandra Sink
=======================

The Cassandra Sink component allows to save Flume-flow events into Cassandra.
It tries to find the defined fields in the event's headers. If a "data" field is defined, it will take the body's event instead of a header.

Configuration
=============

The available config parameters are:

- table: One or more table names separated with commas. Table names must be fully qualified with keyspace (e.g. keyspace1.table1,keyspace2.table2) (Mandatory)

- clusterName: The Cassandra's cluster name. (Default: Test Cluster)

- host: The ip address or the host name of one Cassandra node. (Default: localhost)

- port: The CQL native transport port. Please make sure that this port is open. (Default: 9042)

- username: Database user. (Optional)

- password: Database password. (Optional)

- primaryKey: The primary key of the table. (Optional)

- batchSize: The size to batch insert statement. We recommend 100 as an optimum value to this property. Please do not forget increase the channel.capacity property on your channel component over the sink.batchSize property. (Default: 100)

- consistency: The consistency level for this insert. Default value are QUORUM, available options are described here: [Cassandra data consistency](http://www.datastax.com/documentation/cassandra/2.0/cassandra/dml/dml_config_consistency_c.html) (Default: QUORUM)

- cqlFile: Path to a CQL file with initialization statements such as keyspace and table creation. (Optional)

- itemSeparator: (Default: ,)

- mapValueSeparator: (Default: :)

- mapKeyType: (Default: TEXT)

- mapValueType: (Default: INT)

- listValueType: (Default: TEXT)

Sample Complete-flow Flume config
=================================

The following file describes an example configuration of an flume agent that use a [Spooling directory source](http://flume.apache.org/FlumeUserGuide.html#spooling-directory-source), a [File channel](http://flume.apache.org/FlumeUserGuide.html#file-channel) and our Cassandra-Sink

``` 
    # Name the components on this agent
    agent.sources = spoolSource
    agent.sinks = cassandraSink
    agent.channels = fileChannel

    # Describe/configure the source
    agent.sources.spoolSource.type = spoolDir
    agent.sources.spoolSource.spoolDir = /home/flume/data/files/

    # Describe the sink
    agent.sinks.cassandraSink.clusterName=testCluster
    agent.sinks.cassandraSink.type=com.stratio.ingestion.sink.cassandra.CassandraSink
    agent.sinks.cassandraSink.table=keyspaceTest.tableTest
    agent.sinks.cassandraSink.itemSeparator=|

    # Use a channel which buffers events in file
    agent.channels=fileChannel
    agent.channels.fileChannel.type = file
    agent.channels.fileChannel.checkpointDir=/home/user/flume/channel/check/
    agent.channels.fileChannel.dataDirs=/home/user/flume/channel/data/
    # Please, remember, this value must be greater than sink.batchSize value.
    agent.channels.fileChannel.transactionCapacity=10000

    # Bind the source and sink to the channel
    agent.sources.spoolSource.channels=fileChannel
    agent.sinks.cassandraSink.channel=fileChannel
```
