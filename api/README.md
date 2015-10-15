Stratio Ingestion API
=====================

The Stratio Ingestion API provides a simple REST interface to the Stratio Ingestion component. This service allows 
the workflow management enabling flume agents configuration. 

Architecture
============

This component forwards the incoming workflow requests to Zookeeper allowing create,update,get,search and delete 
workflows. 

A workflow is the representation of one or multiple Flume agents configuration. Typically in Flume and Ingestion you 
need to configure the agents using properties files. A workflow is the representation of those properties files in 
JSON format. This representation is useful also to abstract the Flume complexity allowing to integrate a front-end 
application to configure the Ingestion agents. 

The Ingestion API has been developed using Scala and Spray framework and includes the following sub-modules:

-   core. It has the logic to interact with zookeeper cluster
-   model. Includes the Sources, Sinks, Channels and Transformations representation
-   service. Includes the HTTP interface





