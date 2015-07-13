#!/bin/bash
INGESTION_HOME="${INGESTION_HOME:-/opt/sds/ingestion}"
cd "$(dirname $0)/../"
exec "${INGESTION_HOME}/bin/flume-ng" agent --conf ./conf --conf-file ./conf/collector.properties --name collector -Dflume.monitoring.type=http -Dflume.monitoring.port=34547 -Xmx1024m
cd
