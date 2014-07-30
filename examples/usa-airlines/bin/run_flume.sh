#!/bin/bash

INGESTION_HOME=${INGESTION_HOME:-/opt/stratio/ingestion}
FLUME_HOME=$INGESTION_HOME
DIR=$(dirname $0)/../

cd $DIR
exec $FLUME_HOME/bin/flume-ng agent --conf $DIR/conf --conf-file $DIR/conf/flume-conf.properties --name a -Dflume.monitoring.type=http -Dflume.monitoring.port=34545
