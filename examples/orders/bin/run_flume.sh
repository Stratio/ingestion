#!/bin/sh
INGESTION_HOME="${INGESTION_HOME:-/opt/sds/ingestion}"
### $IP:34545/metrics
cd "$(dirname $0)/../"
PWD= `pwd`

#bin/cleanData.sh
#cp -f data/tmp/* data/input/


exec "${INGESTION_HOME}/bin/flume-ng" agent --conf ./conf --conf-file ./conf/flume-agent1.properties --name a1 -Dlog4j.configuration=file://$PWD/conf/log4j.properties -Dflume.log.file=agent-1.log -Dflume.root.logger=INFO,LOGFILE,console -Dflume.monitoring.type=http -Dflume.monitoring.port=34545 -Xmx1024m

cd
