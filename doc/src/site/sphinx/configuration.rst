Configuration
*************

Agents Configuration
====================

Each Flume-Ingestion uses different files to configure and execute the agent. You can take a look at existing
examples to see some possible configurations:

-   Properties files. Usually in the conf directory you can find the agent configuration files. The file with the workflow configuration has a .properties extension and includes the definition, configuration & mapping of different Ingestion components (Sources, Transformations, Channels and Sinks).

Each different component have specific configuration so if you need to configure any flume component you can check
the flume reference guide (https://flume.apache.org/FlumeUserGuide.html).

Stratio components
==================

-   Stratio sources


    - Stratio irc source

        A Stratio Ingestion source to get data from IRC.

        Available config parameters:

            - host (string, required): target URI
            - port (integer): Port. Default: 6667
            - nick(string, required): Nickname
            - irc-channels (string, required): Comma separated irc-channels without hash. Example: ubuntu, trivial
            - user (string): The username. Is used to register the connection
            - name(string): The realname. Is used to register the connection
            - password(string): Password. Required if you are registered
            - replyPing(boolean): Automatically sends pong when receives a ping. Default: False


    - Stratio redis source

        A Flume source that read data from Redis Pub Sub system with connection Pool. Accept patterns.

        Available config parameters:

            - host (string): Redist host. Default: localhost.
            - port (integer): Redis port. Default: 6379.
            - subscribe (string): Channels to subscribe to. String or comma separated strings.
            - psubscribe (string): Channels to subscribe with given pattern. String or comma separated strings.
            Invalid if subscribe is assigned.
            - charset(string) : Charset. Default: uft-8.
            - pool.<property>: Prefix for pool properties. Set whatever property you want to the connection pool.


    - Stratio rest source

        A Flume source that make a request to a REST service.

        Available config parameters:

            - url: target URL request. Required.
            - method: Method type. Default: GET.
            - applicationType: Application Type. Default: JSON. Possible values: TEXT, JSON.
            - frequency: Frequency to send request to the url in seconds. Default: 10.
            - headers: Headers json. e.g.: { Accept-Charset: utf-8, Date: Tue,15 Nov 1994 08:12:31 GMT} Default: "".
            - body: Body for post request. Default: "".
            - restSourceHandler: Handlder implementation classname. This handler will transform the rest service
            response into one or many events. Default: "com.stratio.ingestion.source.rest.handler.DefaultRestSourceHandler".
            - jsonPath: field path from which we want recovery the response data. Default: "".
            - urlHandler: UrlHandler implementation classname. Currently you have two implementations:

                - DefaultUrlHandler: return url
                - DynamicUrlHandler: check url and replace query params specified as ${param_name}.

            - urlHandlerConfig: json file path where you have to specify how to handler query params. e.g. If you have an
            url such as http://localhost:34545/metrics?from_date=${date}, you have to specify how to parse ${date} param:

            ::

                {
                    "filterHandler": "com.stratio.ingestion.source.rest.url.filter.MongoFilterHandler",
                    "filterConfiguration" :"../conf/filterConfig.json",
                    "urlParamMapper" : "{\"params\":[{\"name\":\"date\", \"default\":\"1970-01-01%2000:00:00\"}]}"
                }


            Currently, there is only one implementation for filterHandler: MongoFilterHandler. In this case, we recovery "date" info from MongoDB dynamically. In order to do that, we specify withtin filterConfig.json file how to access to MongoDB instance and how to parse the field:
            ::

                {
                    "field": "date",
                    "type": "com.stratio.ingestion.source.rest.url.filter.type.DateCheckpointType",
                    "dateFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
                    "mongoUri": "mongodb://socialLoginUser:temporal@180.205.132.228:50017,180.205.132.229:50017,180.205.132.230:50017/socialLogin.checkpoints?replicaset=socialLogin&ssl=true"
                }

            This source will send a request to url target every 'frequency' seconds, retrieve the response and put it into its flume channel.


    - Stratio snmptraps source

        A Flume source that listens to snmp traps.

        Available config parameters:

            - address (string): Address to listen snmp traps. Default: localhost
            - snmpTrapPort (integer): Port to listen snmp traps. Default: 162
            - snmpVersion (string): SNMP Protocol version. Possible values: V1,V2c,V3. Default: V1
            - snmpTrapVersion(string) : SNMP Trap Protocol version. Possible values: V1,V2c,V3. Default: V1
            - encryptionType (string): Encryption. Possible values: SHA, MD5. Default: MD5
            - authenticationType (string): SNMP Authentication. Possible values: AUTH_NOPRIV, NOAUTH_NOPRIV,
            AUTH_PRIV. Default: NOAUTH_NOPRIV
            - username (string): username. Required when authenticationType -> AUTH_NOPRIV, AUTH_PRIV
            - password (string): password. Required When authenticationType -> AUTH_NOPRIV, AUTH_PRIV
            - privacyProtocol (string): Privacy protocol. Required when authenticationType -> AUTH_PRIV. Possible values:
            PrivDES, Priv3DES, PrivAES128, PrivAES192, PrivAES256, PrivAES192With3DESKeyExtension, PrivAES256With3DESKeyExtension. Default: PRIVDES.
            - privacyPassphrase (string): Privacy passphrase. Required when authenticationType -> AUTH_PRIV.


Stratio sinks

    - Stratio cassandra sink

        The Cassandra Sink component allows to save Flume-flow events into Cassandra. It tries to find the defined fields in the event's headers. If a "data" field is defined, it will take the body's event instead of a header.

        The available config parameters are:

            - tables: One or more table names separated with commas. Table names must be fully qualified with keyspace (e.g. keyspace1.table1,keyspace2.table2) (Mandatory)
            - hosts: A comma-separated list of Cassandra hosts. It is recommended to specify at least two host of the
             cluster. The result of the cluster will be auto-discovered. (Default: localhost:9042)
            - username: Database user. (Optional)
            - password: Database password. (Optional)
            - batchSize: The size to batch insert statement. We recommend 100 as an optimum value to this property.
            Please do not forget increase the channel.capacity property on your channel component over the sink.batchSize property. (Default: 100)
            - consistency: The consistency level for this insert. Default value are QUORUM, available options are
            described here: Cassandra data consistency (Default: QUORUM)
            - cqlFile: Path to a CQL file with initialization statements such as keyspace and table creation. (Optional)


    - Stratio decision sink


        The Stratio Decision Sink will insert flume events to an specific stream. The configuration is located in the flume config (see sample below.)

        Available config parameters:

            - kafka: Kafka brokers (comma separated list) where the Stratio Decision/Kafka instance is running
            - zookeeper: Zookeeper quorum where the Stratio Decision/Zookeeper instance is running
            - streamDefinitionFile: stream definition file path (see example below)

        This sink will extract the data from the flume event headers and for each field within the headers map it will create a new stream "field" with the content of the specific header. You must provide a mechanism to parse the content of the flume flow to the event headers (we strongly recommend using morphlines).


    - Stratio druid sink

        The Druid Sink component allows to save Flume-flow events to Druid.

        The available config parameters are:

            - indexService (String, required): Overlord's service name
            - discoveryPath (String, required): Your overlord's druid.discovery.curator.path
            - dimensions (String, required): Comma separated list with event headers you want to stored. Similar to
            columns in relational databases.
            - firehosePattern (String): Firehoses describe the data stream source. Make up a service pattern, include %s
            somewhere in it. This will be used for internal service-discovery purposes, to help druid sink find Druid indexing tasks. By default, druid:firehose:%s.
            - dataSource (String): Source name where events will be stored. Very similar to a table in relational
            databases. By default, sampleSource.
            - aggregators (String): Different specifications of processing over available metrics. By default, count
            aggregator.
            - zookeeperLocation (String): Zookeeper location (hostname:port).By default, 127.0.0.1:2181.
            - timestampField (String): The field name where event timestamp info is extracted from. By default, timestamp.
            - segmentGranularity (Granularity): Time granularity (minute, hour, day, week, month) for loading data at
            query time. Recommended, more than queryGranularity. By default, HOUR.
            - queryGranularity (Granularity): Time granularity (minute, hour, day, week, month) for rollup. At least, less
             than segmentGranularity. Recommended: minute, hour, day, week, month. By default, NONE.
            - period (Period): While reading, events with timestamp older than now minus this value, will be discarded. By
             default, PT10M.
            - partitions (Integer): This is used to scale ingestion up to handle larger streams. By default, 1.
            - replicants (Integer): This is used to provide higher availability and parallelism for queries. By default, 1.
            - baseSleepTime (Integer): Initial amount of time to wait between retries. By default, 1000.
            - maxRetries (Integer): Max number of times to retry. By default, 3.
            - maxSleep (Integer): Max time in ms to sleep on each retry. By default, 30000.
            - batchSize (Integer): Number of events to batch together to be send to our data source. By default, 1000.

        As a recommendation: The normal, expected use cases have the following overall constraints: queryGranularity < windowPeriod < segmentGranularity.


    - Stratio jdbc sink

        Stratio JDBC Sink saves Flume events to any database with a JDBC driver. It can operate either with automatic headers-to-tables mapping or with custom SQL queries.

        The available config parameters are:

            - type (string, required): You should use: com.stratio.ingestion.sink.jdbc.JDBCsink
            - driver (string, required): The driver class (e.g. org.h2.Driver, org.postgresql.Driver). NOTE: Stratio JDBC
            Sink only include H2, Mysql, and Derby drivers. You must add another JDBC drivers to your Flume classpath.
            - sqlDialect (string, required): The SQL dialect of your database. This should be one of the following:
            CUBRID, DERBY, FIREBIRD, H2, HSQLDB, MARIADB, MYSQL, POSTGRES, SQLITE.
            - connectionString (string, required): A valid connection string to a database. Check the documentation for
            your JDBC driver for more information.
            - username (string): A valid database username.
            - password (string): Password.
            - table (string): A table to store your events. This is only used for automatic mapping.
            - sql (string): A custom SQL query to use. If specified, this query will be used instead of automatic mapping.
             E.g. INSERT INTO tweets (text, num_hashtags, timestamp) VALUES (${body:string}, ${header.numberOfHashtags:integer}, ${header.date:timestamp}). Note the variable format: the first part is either body or header.yourHeaderName and then the SQL type.
            - batchSize (integer): Number of events that will be grouped in the same query and transaction. Defaults to 20.


    - Stratio kafka sink

        Kafka Sink send events to Kafka.

        The available config parameters are:

            - topic (string): Name of topic where event will be sent to. Required.
            - writeBody (boolean): true to send body in raw String format and false to send headers in json String format.
             Default: False (Send only headers).
            - kafka.<producer-property> (string): This sink accept any kafka producer property. Just write it after prefix
             "kafka.". Example: kafka.metadata.broker.list



    - Stratio mongo sink

        The MongoDB Sink component allows to save Flume-flow events to MongoDB. It can parse both event body and headers.

        The available config parameters are:

            - dynamic (boolean): If true, the dynamic mode will be enabled and the database and collection to use will be
            selected by the event headers. Defaults to false.
            - dynamicDB (string): Name of the event header that will be looked up for the database name. This will only
            work when dynamic mode is enabled. Defaults to "db".
            - dynamicCollection (string): Name of the event header that will be looked up for the collection name. This
            will only work when dynamic mode is enabled. Defaults to "collection".
            - mongoUri (string, required): A Mongo client URI defining the MongoDB server address and, optionally
            authentication, default database and collection. When dynamic mode is enabled, the collection defined here will be used as a fallback.
            - mappingFile (string): Path to a JSON schema to be used for type mapping purposes. See the Type Mapping
            section for further information.
