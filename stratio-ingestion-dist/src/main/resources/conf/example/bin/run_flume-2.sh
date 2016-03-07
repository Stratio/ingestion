#!/bin/bash
INGESTION_HOME="${INGESTION_HOME:-/opt/sds/ingestion}"

echo "Ingestion Home = $INGESTION_HOME"

cd "$(dirname $0)/../"

exec "${INGESTION_HOME}/bin/flume-ng" agent --conf ./conf --conf-file ./conf/flume-conf2.properties --name a2  -Dflume.root.logger=INFO,console


