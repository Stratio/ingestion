#!/bin/sh
INGESTION_HOME="${INGESTION_HOME:-/opt/sds/ingestion}"
### $IP:34546/metrics
cd "$(dirname $0)/../"

exec "${INGESTION_HOME}/bin/flume-ng" agent --conf ./conf --conf-file ./conf/flume-agent2.properties --name a2 -Dflume.root.logger=INFO,console -Dflume.monitoring.type=http -Dflume.monitoring.port=34546 -Xmx1024m &

cd
