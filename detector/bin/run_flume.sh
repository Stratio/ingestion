#!/bin/bash
INGESTION_HOME="${INGESTION_HOME:-/opt/sds/ingestion}"
export FLUME_JAVA_OPTS="-Xmx1024m"
cd "$(dirname $0)/../"
exec "${INGESTION_HOME}/bin/flume-ng" agent --conf ./conf --conf-file ./conf/flume-conf.properties --name a -Dflume.monitoring.type=http -Dflume.monitoring.port=34545
