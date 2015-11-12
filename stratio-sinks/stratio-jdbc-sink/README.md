Stratio JDBC Sink
=================

Stratio JDBC Sink saves Flume events to any database with a JDBC driver.
It can operate either with automatic headers-to-tables mapping or with custom SQL queries.


Configuration
=============

The available config parameters are:

- `type` *(string, required)*: You should use: com.stratio.ingestion.sink.jdbc.JDBCsink

- `driver` *(string, required)*: The driver class (e.g. `org.h2.Driver`, `org.postgresql.Driver`). **NOTE: Stratio JDBC Sink only include H2, Mysql, and Derby drivers. You must add another JDBC drivers to your Flume classpath.**

- `sqlDialect` *(string, required)*: The SQL dialect of your database. This should be one of the following: `CUBRID`, `DERBY`, `FIREBIRD`, `H2`, `HSQLDB`, `MARIADB`, `MYSQL`, `POSTGRES`, `SQLITE`. 

- `connectionString` *(string, required)*: A valid connection string to a database. Check the documentation for your JDBC driver for more information.

- `username` *(string)*: A valid database username.

- `password` *(string)*: Password.

- `table` *(string)*: A table to store your events. *This is only used for automatic mapping.*

- `sql` *(string)*: A custom SQL query to use. If specified, this query will be used instead of automatic mapping. E.g. `INSERT INTO tweets (text, num_hashtags, timestamp) VALUES (${body:string}, ${header.numberOfHashtags:integer}, ${header.date:timestamp})`. Note the variable format: the first part is either `body` or `header.yourHeaderName` and then the SQL type.

- `batchSize` *(integer)*: Number of events that will be grouped in the same query and transaction. Defaults to 20.

Automatic mapping
=================

If no custom SQL option is given, the database schema will be analyzed. Then, for each event, each header will be mapped to a table field with the exact same name (case insensitive), if any. Type conversion will be done automatically to match the corresponding SQL type.

Note that event body is NOT mapped automatically.

Sample Flume config
===================

The following file describes an example configuration of an Flume agent that uses a [Spooling directory source](http://flume.apache.org/FlumeUserGuide.html#spooling-directory-source), a [File channel](http://flume.apache.org/FlumeUserGuide.html#file-channel) and Stratio JDBC Sink.

``` 
    # Name the components on this agent
    agent.sources = r1
    agent.sinks = jdbcSink
    agent.channels = c1

    # Describe/configure the source
    agent.sources.r1.type = spoolDir
    agent.sources.r1.spoolDir = /home/flume/data/files/

    # Describe the sink
    agent.sinks.jdbcSink.type = com.stratio.ingestion.sink.jdbc.JDBCSink
    agent.sinks.jdbcSink.connectionString = jdbc:h2:/tmp/jdbcsink_test
    agent.sinks.jdbcSink.table = test
    agent.sinks.jdbcSink.batchSize = 10 

    # Use a channel which buffers events in file
    agent.channels = c1
    agent.channels.c1.type = file
    agent.channels.c1.checkpointDir = /home/user/flume/channel/check/
    agent.channels.c1.dataDirs = /home/user/flume/channel/data/
    # Remember, transactionCapacity must be greater than sink.batchSize.
    agent.channels.c1.transactionCapacity=10000 

    # Bind the source and sink to the channel
    agent.sources.r1.channels = c1
    agent.sinks.jdbcSink.channel = c1
```

