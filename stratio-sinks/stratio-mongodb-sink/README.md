Stratio MongoDB Sink
====================

The MongoDB Sink component allows to save Flume-flow events to MongoDB. It can parse both event body and headers.


Get it
======

Get the tarball
---------------

Download the latest release and extract it in the `plugins.d` directory of your Flume distribution.


Get it with Maven
-----------------

```xml
   <dependency>
     <groupId>com.stratio.ingestion</groupId>
     <artifactId>stratio-mongodb-sink</artifactId>
     <version>0.0.1-SNAPSHOT</version>
   </dependency>
```


Configuration
=============

The available config parameters are:

- `dynamic` *(boolean)*: If true, the dynamic mode will be enabled and the database and collection to use will be selected by the event headers. Defaults to false.

- `dynamicDB` *(string)*: Name of the event header that will be looked up for the database name. This will only work when dynamic mode is enabled. Defaults to "db".

- `dynamicCollection` *(string)*: Name of the event header that will be looked up for the collection name. This will only work when dynamic mode is enabled. Defaults to "collection".

- `mongoUri` *(string, required)*: A [Mongo client URI](http://api.mongodb.org/java/current/com/mongodb/MongoClientURI.html) defining the MongoDB server address and, optionally, default database and collection. When dynamic mode is enabled, the collection defined here will be used as a fallback.

- `mappingFile` *(string)*: Path to a [JSON schema](http://json-schema.org/) to be used for type mapping purposes. See the *Type Mapping* section for further information. 

Type mapping
============

The MongoDB Sink will always work out of the box by trying to parse everything as JSON and falling back to plain strings when parsing fails. However, when more control is needed over parsing and type mapping, a JSON schema can be provided.

This JSON schema has some custom extensions:

- `bodyField` *(string)*: If defined, the event body will be saved to this field in MongoDB. If set to the empty string and `bodyType` to `object`, then body will be parsed as JSON and merged with the MongoDB document. Defaults to `data`.

- `bodyType` *(string)*: Defines the type of the event body. If set to `null`, body will be descarded. Defaults to `binary`.

- `bodyEncoding` *(string)*: When `bodyType` is `binary`, this option can be set to `raw` to store raw bytes or `base64` to read body as a base64-encoded byte array. When `bodyType` is `string`, this option can be set to the string encoding. Defaults to `raw` and `UTF-8`.

- `mappedFrom` *(string)*: This can be defined at the property level. If the name of a field is differs between the event headers and the document collection, `mappedFrom` can specify the name of the field in the event headers.

The following extra types are defined to match MongoDB's BSON types:

- `int32`, `int64` and `double`: These should be used instead of the JSON types `integer` or `numeric`.

- `date`: Matches MongoDB date type. This sink expects dates to be formatted as milliseconds since January 1, 1970, 00:00:00 GMT. If a formatted date is expected, an additional field `dateFormat` can be provided with a [date format string](http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html) (see example below).

- `binary`: Binary data. It will be parsed as base64-encoded. It can be combined with an additional field `encoding` (see example below).

- `objectid`: See MongoDB documentation for [ObjectId](http://docs.mongodb.org/manual/reference/object-id/#core-object-id-class).

Type mapping examples
---------------------

### Default mapping (no JSON schema)

Let's see how an event is mapped to a MongoDB document by default, with no JSON schema specified.

**Event:**

| Header     | Value                    |
| ---------- | -----------------------  |
| myString   | "string"                 |
| myString2  | string                   |
| myString3  | "1234"                   |
| myIntSmall | 123                      |
| myIntBig   | 2147483649               |
| myDouble   | 1.0                      |
| myBoolean  | true                     |
| myArray    | [1, 2.0, "3"]            |
| myObj      | {"foo": "bar"}           |
| myDate     | 1402502196000            |
| myDate2    | 2014/12/30               |
| myBinary   | U3RyYXRpbw==             |
| myObjectId | 507c7f79bcf86cd7994f6c0e |
| **BODY**   | {"myString4": "string4"} |

**Document:**

```json
{
  "myString": "string",
  "myString2": "string",
  "myString3": "1234",
  "myString4": "string4",  // <--- Note that this field came from a JSON body.
  "myIntSmall": 123,       // <--- INT32
  "myIntBig": 2147483649,  // <--- INT64
  "myDouble": 1.0,
  "myBoolean": true,
  "myArray": [1, 2.0, "3"],
  "myObj": {
    "foo": "bar"
  },
  "myDate": Date(2014-06-11T15:56:36),
  "myDate2": "2014/12/30"
  "myBinary": "U3RyYXRpbw==",
  "myObjectId": "507c7f79bcf86cd7994f6c0e",
  "data": BinData([0x7b, 0x22, 0x6d, 0x79, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x34, 0x22, 0x3a, 0x20, 0x22, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x34, 0x22, 0x7d]) 
}
```

### With JSON schema

Now let's specify a JSON schema using a fair amount of custom options:

```json
{
  "title": "Example Schema",
  "type": "object",
  "bodyType": "object",
  "bodyField": "data",
  "additionalProperties": false,
  "properties": {
    "myString": {
      "type": "string"
    },
    "myStringMapped": {
      "type": "string",
      "mappedFrom": "myString2"
    },
    "myIntSmall": {
      "type": "int32"
    },
    "myIntBig": {
      "type": "int64"
    },
    "myDouble": {
      "type": "double"
    }
    "myBoolean": {
      "type": "boolean"
    },
    "myArray": {
      "type": "array"
    },
    "myObj": {
      "type": "object"
    },
    "myDate": {
      "type": "date"
    },
    "myDate2": {
      "type": "date",
      "dateFormat": "yyyy/MM/dd"
    }
    "myBinary": {
      "type": "binary",
      "encoding": "base64"
    },
    "myObjectId": {
      "type": "objectid"
    }
  }
}
```

If we take the same event as in the previous example, it will be converted to a document conforming to the following JSON:

```json
{
  "myString": "string",
  "myStringMapped": "string",
  "myIntSmall": 123,       // <--- INT32 (forced)
  "myIntBig": 2147483649,  // <--- INT64 (forced)
  "myDouble": 1.0,
  "myBoolean": true,
  "myArray": [1, 2.0, "3"],
  "myObj": {
    "foo": "bar"
  },
  "myDate": Date(2014-06-11T15:56:36Z),
  "myDate2": Date(2014-12-31T00:00:00Z),
  "myBinary": BinData([0x53, 0x74, 0x72, 0x61, 0x74, 0x69, 0x6f]),
  "myObjectId": ObjectId(507c7f79bcf86cd7994f6c0e),
  "data": {
     "myString4": "string4"
  }
}
```

Note that any field not specified in the JSON schema was ignored. This is because `additionalProperties` was set to `false`.



Sample Complete-flow Flume config
=================================

The following file describes an example configuration of an Flume agent that uses a [Spooling directory source](http://flume.apache.org/FlumeUserGuide.html#spooling-directory-source), a [File channel](http://flume.apache.org/FlumeUserGuide.html#file-channel) and our MongoDB Sink

``` 
    # Name the components on this agent
    agent.sources = r1
    agent.sinks = mongoSink
    agent.channels = c1

    # Describe/configure the source
    agent.sources.r1.type = spoolDir
    agent.sources.r1.spoolDir = /home/flume/data/files/

    # Describe the sink
    agent.sinks.mongoSink.type = com.stratio.ingestion.sink.mongodb.MongoSink
    agent.sinks.mongoSink.mongoUri = mongodb://localhost/testdb.testcoll
    agent.sinks.mongoSink.dynamic = false
    agent.sinks.mongoSink.batchSize = 200 
    agent.sinks.mongoSink.mappingFile = /home/flume/conf/schema.json

    # Use a channel which buffers events in file
    agent.channels = c1
    agent.channels.c1.type = file
    agent.channels.c1.checkpointDir = /home/user/flume/channel/check/
    agent.channels.c1.dataDirs = /home/user/flume/channel/data/
    # Remember, transactionCapacity must be greater than sink.batchSize
    agent.channels.c1.transactionCapacity=10000

    # Bind the source and sink to the channel
    agent.sources.r1.channels = c1
    agent.sinks.mongoSink.channel = c1

