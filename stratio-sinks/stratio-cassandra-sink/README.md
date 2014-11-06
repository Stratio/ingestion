Stratio Cassandra Sink
=======================

The Cassandra Sink component allows to save Flume-flow events into Cassandra.
It tries to find the defined fields in the event's headers. If a "data" field is defined, it will take the body's event instead of a header.

Configuration
=============

The available config parameters are:

- clusterName: The Cassandra's cluster name. (Default: TestCluster)

- keyspace: The Keyspace name which allocates the table. (Default: keyspace_logs)

- host: The ip address or the host name of one Cassandra node. (Default: localhost)

- table: The table name. (Default: table_logs)

- username: Database user. (Optional)

- password: Database password. (Optional)

- primaryKey: The primary key of the table. (Optional)

- batchSize: The size to batch insert statement. We recommend 100 as an optimum value to this property. Please do not forget increase the channel.capacity property on your channel component over the sink.batchSize property. (Default: 100)

- port: The CQL native transport port. Please make sure that this port is open. (Default: 9042)

- consistency: The consistency level for this insert. Default value are QUORUM, available options are described here: [Cassandra data consistency](http://www.datastax.com/documentation/cassandra/2.0/cassandra/dml/dml_config_consistency_c.html) (Default: QUORUM)

- keyspaceStatement: The keyspace creation statement. (Optional)

- tableStatement: The table creation statement. (Optional)

- definitionFile: The absolute route to the JSON definition file which defines the Cassandra table. Explained below. (Optional)

The following configuration is used when the definitionFile is not set it. Explained below.

- dateFormat: (Default: dd/MM/yyyy)

- itemSeparator: (Default: ,)

- mapValueSeparator: (Default: ;)

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
    agent.sinks.cassandraSink.keyspace=test
    agent.sinks.cassandraSink.clusterName=testCluster
    agent.sinks.cassandraSink.type=com.stratio.ingestion.sink.cassandra.CassandraSink
    agent.sinks.cassandraSink.table=tableTest
    agent.sinks.cassandraSink.separator=,
    agent.sinks.cassandraSink.dateFormat=dd/MMM/yyyy:HH:mm:ss z

    # Use a channel which buffers events in file
    agent.channels = fileChannel
    agent.channels.fileChannel.type = file
    agent.channels.fileChannel.checkpointDir = /home/user/flume/channel/check/
    agent.channels.fileChannel.dataDirs = /home/user/flume/channel/data/
    # Please, remember, this value must be greater than sink.batchSize value.
    agent.channels.fileChannel.transactionCapacity=10000

    # Bind the source and sink to the channel
    agent.sources.spoolSource.channels = fileChannel
    agent.sinks.cassandraSink.channel = fileChannel

``` 

JSON definition file
====================

This file describe the columns of the ingested table. There are two types of fields: the header ones and the body ones.
The body fields must be in the same order as the ingested file, namely if the ingested file for the person table as follow: lastName, fisrtName, age the JSON file must describe first the column lastName, then the column fristName and lately the column age. So it isn't necessary that the first field be the table's primary key.

For each columns you must define the following fields:

- type: The Cassandra's field type. This field must be one of the following options, some of this options force you to fill other properties for this field:`TEXT`,`VARCHAR`,`VARINT`,`ASCII`,`BOOLEAN`,`DECIMAL`,`DOUBLE`,`FLOAT`,`INET`,`INT`,`COUNTER`,`LIST`,`MAP`,`SET`,`TIMESTAMP`,`UUID` or `BIGINT`.

- columnName: The Cassandra's columns name.

- dateFormat: If the type, the map key type , the map value or the listValueType is a `TIMESTAMP`, you must define the date format using this [pattern:](http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html). If you has several `TIMESTAMP` columns you can use differents dateFormat but if you have a map type column with a `TIMESTAMP` mapKeyType and `TIMESTAMP` mapValueType both have to have the same dateFormat.

- itemSeparator: The regular expression that separate each value in a `LIST` or `SET` type inside the ingest file. This property only must the defined if the columnType is `LIST` or `SET`.

- mapValueSeparator: The regular expression that separate each pair key-value in a map type inside the ingest file. This property only must the defined if the columnType is `MAP`.

- mapKeyType: The key's type in a column `MAP` type. This field must be one of the following options `TEXT`,`VARCHAR`,`VARINT`,`ASCII`,`BOOLEAN`,`DECIMAL`,`DOUBLE`,`FLOAT`,`INET`,`INT`,`COUNTER`,`TIMESTAMP`,`UUID` or `BIGINT`. This property only must the defined if the columnType is `MAP`.

- mapValueType: The value's type in a column `MAP` type. This field must be one of the following options `TEXT`,`VARCHAR`,`VARINT`,`ASCII`,`BOOLEAN`,`DECIMAL`,`DOUBLE`,`FLOAT`,`INET`,`INT`,`COUNTER`,`TIMESTAMP`,`UUID` or `BIGINT`. This property only must the defined if the columnType is `MAP`.

- listValueType: The value's type in a column `LIST` or `SET` type. This field must be one of the following options `TEXT`,`VARCHAR`,`VARINT`,`ASCII`,`BOOLEAN`,`DECIMAL`,`DOUBLE`,`FLOAT`,`INET`,`INT`,`COUNTER`,`TIMESTAMP`,`UUID` or `BIGINT`. This property only must the defined if the columnType is  `LIST` or `SET`.

We highly recommend verify this file using some verify application or web pages as [JSONLint](http://jsonlint.com/).


#### Sample JSON definition file
This is an example for a 4 columns table. The column types are field1:TEXT, field2:DOUBLE, field3:MAP(timestamp{dateFormat ddMMyyyy},double) and field4:LIST(double)
``` 
  {
    "fieldDefinitions": [
        {
            "type": "TEXT",
            "columnName": "field1",
            "dateFormat": "",
            "itemSeparator": "",
            "mapValueSeparator": "",
            "mapKeyType": "",
            "mapValueType": "",
            "listValueType": ""
        },
        {
            "type": "DOUBLE",
            "columnName": "field2",
            "dateFormat": "",
            "itemSeparator": "",
            "mapValueSeparator": "",
            "mapKeyType": "",
            "mapValueType": "",
            "listValueType": ""
        },
        {
            "type": "MAP",
            "columnName": "field3",
            "dateFormat": "ddMMyyyy",
            "itemSeparator": "",
            "mapValueSeparator": ";",
            "mapKeyType": "TIMESTAMP",
            "mapValueType": "DOUBLE",
            "listValueType": ""
        },
        {
            "type": "LIST",
            "columnName": "field4",
            "dateFormat": "",
            "itemSeparator": "\\|",
            "mapValueSeparator": "",
            "mapKeyType": "",
            "mapValueType": "",
            "listValueType": "DOUBLE"
        }
    ]
   }
``` 
