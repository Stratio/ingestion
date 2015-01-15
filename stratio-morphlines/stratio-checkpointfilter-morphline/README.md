Stratio Checkpoint Filter Morphline
=======================

The stratio checkpointFilter command works as a common timer filter with some improvements.

When command starts, it gets the last checkpoint stored a checkpoint handler. By  now, only MongoCheckpointFilterHandler is avalible.

Then, the command uses that value to filter the records. It will compare that value with parametrized record field.

Finally, the checkpoint value will be updated periodically to prevent existent records  are stored again when fatal error occurs.  Source Selector  with "checkpoint" field is needed at morphline config file.

Lets explain the fields:

- handler: handler to recover/update the last checkpoint value (e.g. MongoCheckpointFilterHandler)
- field: field where checkpoint value is stored
- type: checkpoint value type (e.g. DataCheckpointType)
- format: if necessary, checkpoint value format
- chunksize: set the number of processed records without checkpoint value updating.

Example:


    {
	    checkpointFilter {
            handler: com.stratio.ingestion.morphline.checkpointfilter.handler.MongoCheckpointHandler
            field: date
            type: com.stratio.ingestion.morphline.checkpointfilter.type.DateCheckpointType
            format: "dd/MM/yyyy"
            chunksize: 1000
	    }
	}


Additionally, source selector is needed at morphline configuration file:

    agent.sources.httpSource.selector.type=multiplexing
    agent.sources.httpSource.selector.header=checkpoint
    agent.sources.httpSource.selector.mapping.true=checkpointsChannel otherChannel
    agent.sources.httpSource.selector.default=otherChannel