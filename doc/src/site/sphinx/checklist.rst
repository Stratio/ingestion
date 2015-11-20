Production Checklist & Trouble Shooting Guide
=============================================

The most important points to pay attention in Ingestion agents configurations are:

-   Connectivity problems with remote systems. If you're getting or writing data in an external platform double check you don't have a connectivity problem before to run your agents.

-   Problems with transformations. If you're creating your custom transformations it's a common problem that if any morphline command fails, you don't see any data in your sinks. Go step by step adding your morphlines commands checking that are producing the expected results. Also is useful to use the logDebug, logWarn, logInfo commands to print the results after a morphline transformation:
::


    { logDebug { format : "Test: {}", args : ["@{}"] } }


-   Sources, Channels and Sinks are configured properly. Check that sinks have associated the required channels
    filling their 'channels' field, and check also that channels are associated with sources filling the 'sources'
    field. To check this there are validations that will return errors if a field is empty or it's not valid.
    There are similar validations for all the required fields of each component and if any of the component is empty.
    To check that errors you can take a look in /Ingestion/logs folder.

-   Some sinks write data that comes in event headers, other sinks use information of event body. So be
    sure that you are transforming the data properly and you're not seeing results in your datastore, review the sink
    configuration to check if this Sink require information in headers or body.

-   To ensure the network connectivity be sure to open the necessary ports (check this with netstat command in Linux)
    Also check that your Zookeeper and Kafka instances are running correctly before run the flume configuration.