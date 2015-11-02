Architecture Guide
==================

Each agent consists of three major components:

-   Sources are active components that receive data for some other application producing that data.

-   Channels are passive components that buffer data received by the agent, behaving like queues.

-   Sinks poll their respective channels continuously to read and remove events.

 .. image:: images/ingestion_architecture.jpg
    :width: 70%
    :align: center


**Ingestion components**

-   Sources
    -  REST (calls to a REST service)
    -  Redis (data from Redis Pub/Sub system)
    -  IRC (IRC chat protocol)
    -  SNMP traps (SNMP asynchronous notifications)


-   Sinks
    -   Cassandra
    -   MongoDB
    -   Druid
    -   JDBC
    -   Kafka
    -   Stratio Decision (Complex Event Processing engine)


-   Flume Interceptors

    Timestamp, Host, Static, UUID, Search & Replace, Regex and Morphline Interceptor

-   Morphline Interceptor

    Created by Cloudera. Lots of command possibilities: addValue, addLocalHost, contains, equals, if, findReplace,
    Java and Grok

-   Custom Morphlines

    Developed with Apache Kite.

-   Data transporter and collector: `Apache Flume`_
-   Data extractor and transformer: `Morphlines`_


**Sources, Channels and Sink**

-   Sources
    The entity through which data enters into Flume. Sources either actively poll for data or passively wait for data to
    be delivered to them. A variety of sources allow data to be collected, such as log4j logs and syslogs.

-   Sinks
    The entity that delivers the data to the destination. A variety of sinks allow data to be streamed to a range of destinations.
    One example is the HDFS sink that writes events to HDFS.

-   Channels
    The conduit between the Source and the Sink. Sources ingest events into the channel and the sinks drain the channel.

-   Event
    A singular unit of data that is transported by Flume (typically a single log entry)

-   Agent
    Any physical Java virtual machine running Flume. It is a collection of sources, sinks and channels.

-   Client
    The entity that produces and transmits the Event to the Source operating within the Agent.


-   Sink processors
 .. image:: images/ingestion_sinks_processors.jpg
    :width: 70%
    :align: center

-   Multiple tier topologies
 .. image:: images/ingestion_topologies.jpg
    :width: 70%
    :align: center

-   Ingestion arquitecture
 .. image:: images/ingestion_arquitecture.jpg
    :width: 70%
    :align: center


**Summary**

*   Clients send events to agents.
*   Each agent hosts flume components: source, interceptors, channel selectors, channels, sink processors and sinks.
*   Sources and sinks are active components, channels are passive.
*   Source accepts events, passes them through the configured interceptor(s), and if not filtered, puts them on
    channel(s) selected by the configured channel selector.
*   Sink processor identifies a sink to invoke, that can take events from channel and send them to its next hop
    destination.
*   Channel operations are transactional to guarantee one-hop delivery semantics.
*   Channel persistence provides end-to-end reliability.


