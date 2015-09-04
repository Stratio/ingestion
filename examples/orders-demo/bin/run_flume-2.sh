#!/bin/sh
INGESTION_HOME="${INGESTION_HOME:-/opt/sds/ingestion}"
export FLUME_HOME=$INGESTION_HOME
### $IP:34546/metrics
cd "$(dirname $0)/../"

rm -rf data/agent2/sink/file/*
rm -f data/input-json/*
cp -f data/tmp/* data/input-json/

exec "${INGESTION_HOME}/bin/flume-ng" agent --conf ./conf --conf-file ./conf/flume-agent2.properties --name a2 --plugins-path ${INGESTION_HOME}/plugins.d/ -Dflume.root.logger=DEBUG,console -Dflume.monitoring.type=http -Dflume.monitoring.port=34546 -Xmx1024m -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005
#exec "${INGESTION_HOME}/bin/flume-ng" agent --conf ./conf --conf-file ./conf/flume-agent2.properties --name a2 --plugins-path ${INGESTION_HOME}/plugins.d/ -Dflume.root.logger=DEBUG,console -Dflume.monitoring.type=http -Dflume.monitoring.port=34546 -Xmx1024m 

cd
