#!/bin/bash
INGESTION_HOME="${INGESTION_HOME:-/opt/sds/ingestion}"

#export FLUME_JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=1044"
echo -e "Ingestion Home = $INGESTION_HOME"

cd "$(dirname $0)/../"

rm -rf data/spooldir/*
rm -rf data/spooldir/.f*
cp -rf data/backup/* data/spooldir/

exec "${INGESTION_HOME}/bin/flume-ng" agent --conf ./conf --conf-file ./conf/flume-conf.properties --name a -Dflume.monitoring.type=http -Dflume.monitoring.port=34545 -Xmx1024m -Dlog4j.configuration=file:///home/aitor/Projects/Ingestion/examples/decision/conf/log4j.properties

