Stratio Metadata Intereceptor
=============================

This interceptor create a new schema in Stratio Metadata Schema Registry. You can use several Metadata Interceptors to register different schemas in Metadata Schema Registry.
In order to use this interceptor, you need to add this fields:

type	    –	The component type name has to be com.stratio.ingestion.interceptor.metadata
schemaName  -   The name of the schema to be registered
schema      -   The schema to be registered, this have to be a valid Json, in other case you will receive an error

Example
=======

```
a1.sources = r1
a1.channels = c1
a1.sources.r1.channels =  c1
a1.sources.r1.type = seq
a1.sources.r1.interceptors = metadata
a1.sources.r1.interceptors.metadata.type = com.stratio.ingestion.interceptor.metadata
a1.sources.r1.interceptors.metadata.schemaName = schemaExample
a1.sources.r1.interceptors.metadata.schema = { "key" : "value" }
```