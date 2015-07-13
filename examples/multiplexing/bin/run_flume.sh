#!/bin/bash
INGESTION_HOME="${INGESTION_HOME:-/opt/sds/ingestion}"
cd "$(dirname $0)/../"
exec "${INGESTION_HOME}/bin/flume-ng" agent --conf ./conf --conf-file ./conf/flume-conf.properties --name agent1 -Dflume.monitoring.type=http -Dflume.monitoring.port=34545 -Xmx512m
cd
