#!/bin/sh -x
INGESTION_HOME="${INGESTION_HOME:-/opt/sds/ingestion}"
export FLUME_HOME=$INGESTION_HOME
### $IP:34546/metrics
cd "$(dirname $0)/../"

#rm -rf data/agent2/sink/file/*


#exec "${INGESTION_HOME}/bin/flume-ng" agent --conf ./conf --conf-file ./conf/flume-agent2.properties --name a2 --plugins-path ${INGESTION_HOME}/plugins.d/ -Dflume.root.logger=INFO,console -Dflume.monitoring.type=http -Dflume.monitoring.port=34546 -Xmx1024m

exec "${INGESTION_HOME}/bin/flume-ng" agent --conf ./conf --conf-file ./conf/flume-agent2.properties --name a2 --plugins-path ${INGESTION_HOME}/plugins.d/ -Dlog4j.configuration=file://$PWD/conf/log4j.properties -Dflume.log.file=agent-2.log -Dflume.root.logger=INFO,LOGFILE,console -Dflume.monitoring.type=http -Dflume.monitoring.port=34546 -Xmx1024m

cd

