Stratio ElasticSearch Serializer
================================

This serializer creates the index if it does not exist in ElasticSearch. The index is created with a mapping defined by the user.

Mapping example
===============

```
{ 
	"properties" : {
	    "@timestamp": {
	        "type": "date",
	        "format": "dateOptionalTime"
	    },
	    "log_bytes_returned": {
	         "type": "integer"
	    },
	    "log_host": {
	        "type": "string",
	        "index": "not_analyzed"
	    }
	}
}
```  
