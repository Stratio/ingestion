#!/bin/sh -x

WORKFLOW_NAME=$1
FOLDER=${2:-${WORKFLOW_TOOLS_HOME}/conf}

TOOLS_OPTS="-Dprovider.path=$FOLDER"
export TOOLS_OPTS

"${WORKFLOW_TOOLS_HOME}/bin/export-workflow" download $WORKFLOW_NAME

cd $FOLDER/$WORKFLOW_NAME
"${INGESTION_HOME}/bin/flume-ng" agent --conf . --conf-file $WORKFLOW_NAME --name $WORKFLOW_NAME -f ./$WORKFLOW_NAME/$WORKFLOW_NAME -Dflume.root.logger=DEBUG,console -Dlog4j.configuration=file://${INGESTION_HOME}/conf/log4j.properties
cd -
