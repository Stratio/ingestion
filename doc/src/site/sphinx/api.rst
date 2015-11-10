APIs for writing custom transformations
***************************************

Stratio Ingestion allow transform the input data in different ways. You can find some usage examples on Ingestion project page (examples folder):
 https://github.com/Stratio/Ingestion/

Those are the different options:


Flume Interceptors
==================

Apache Flume includes some ready to use transformations allowing as to modify, enrich and/or drop events in flight. There are different existing interceptors supported by Apache Flume:
Timestamp, Host, Static, UUID, Search & Replace, Regex and Morphline Interceptor

You can find the complete list on:
https://flume.apache.org/FlumeUserGuide.html#flume-interceptors

Example of timestamp interceptor added to a flume agent properties file


::

 a1.sources = r1
 a1.channels = c1
 a1.sources.r1.channels =  c1
 a1.sources.r1.type = seq
 a1.sources.r1.interceptors = i1
 a1.sources.r1.interceptors.i1.type = timestamp




Morphline Interceptors
======================

Morphlines is an open source framework that reduces the time and efforts to transform data. Morphlines are available using the Apache Kite SDK, and allow us to create our own data transformation process using a set of commands. There are a lot of command possibilities:
addValue, addLocalHost, contains, equals, if, findReplace, Java and Grok

You can find the Morphlines reference guide on:
http://kitesdk.org/docs/1.1.0/morphlines/morphlines-reference-guide.html

In this link you can find an existing Morphline interceptor using Kite SDK:
https://github.com/Stratio/Ingestion/blob/master/examples/cassandra-hbase/conf/interceptor.conf

The morphlines interceptors can added to the interceptor.conf properties files (you can define your own interceptor.conf file with a different name. When you configure your source in the Ingestion properties file, you can specify the interceptor file where all those interceptors are defined. Example:

::

 a.sources.avroSource.interceptors = morphlineinterceptor
 a.sources.avroSource.interceptors.morphlineinterceptor.type = org.apache.flume.sink.solr.morphline.MorphlineInterceptor$Builder
 a.sources.avroSource.interceptors.morphlineinterceptor.morphlineFile = conf/interceptor.conf
 a.sources.avroSource.interceptors.morphlineinterceptor.morphlineId = morphline1



Stratio Custom Interceptors
===========================

Stratio Ingestion provides some custom interceptors ready to use in your transformation process:

-   **Commons**:

    *   Calculator: Make operations between fields and/or static numbers.

    *   FieldFilter: Filter fields including or excluding.

    *   ReadXml: Read xml from header or body, and apply XPath expression.

    *   RelationalFilter: Drop fields if they don't accomplish a relational condition.

    *   Rename: Rename a field.

    *   TimeFilter: Filter a time field between specified dates.

    *   ContainsAnyOf: Command that succeeds if all field values of the given named fields contains any of the given values and fails otherwise. Multiple fields can be named, in which case a logical AND is applied to the results.

-   **GeoIP**: Command that works as the kite one. It will save the iso code and the longitude-latitude pair in two header fields.
-   **GeoLocateAirports**: Get the longitude and latitude of an airport from its airport code (from origin and destination).
-   **NLP**: Command that detects the language of a specific header and puts the ISO_639-1 code into another header.
-   **WikipediaCleaner**: Command that cleans the wikipedia markup from a text.
-   **CheckpointFilter**: Get the last checkpoint value by parametrized handler and filter records by paramentrized field too. Periodically update checkpoints values.
-   **LDAP**: Extract RDN's from an LDAP String into separated headers.

You can find more information about how to use those interceptors on Morphlines project page:
http://docs.stratio.com/modules/morphlines/development/


Your own Custom Interceptors
============================

Also you can build your own custom Interceptors using Java code following those steps:

*   Implement the CommandBuilder interface class.
*   Implement or override the getNames method: get the name of the command.
*   Implement or override the build method: define how to config the morphline, its parent, child and context. Here you must create an extended class for AbstractCommand who override the doProcess method (here we define the transformation).

You can find an example about how to build a interceptor on Morphlines project page:
https://github.com/Stratio/Morphlines/blob/master/geolocateairport/src/main/java/com/stratio/morphlines/geolocateairports/AirportLatLonBuilder.java