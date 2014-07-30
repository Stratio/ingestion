#!/bin/bash

DIR=$(dirname $0)/../
LOG_DIR=logs
LOG_FEED_DATA=$LOG_DIR/feed_data.sh.log
LOG_RUN_FLUME=$LOG_DIR/run_flume.sh.log

cd $DIR
mkdir -p $LOG_DIR

echo "Changed working directory to $DIR"
echo "Running feed_data.sh (logging to $LOG_FEED_DATA)"
bin/feed_data.sh &> $LOG_FEED_DATA &
PID_FEED=$!
if [[ ! -e conf/flume-conf.properties ]] ; then
	echo "No Flume configuration found, copying conf/flume-conf-elasticsearch.properties to conf/flume-conf.properties."
	cp conf/flume-conf-elasticsearch.properties conf/flume-conf.properties
fi
echo "Running run_flume.sh (logging to $LOG_RUN_FLUME)"
bin/run_flume.sh &> $LOG_RUN_FLUME &
PID_RUN=$!
while true ; do
	if ! ps -p $PID_RUN > /dev/null 2>&1 ; then
		echo "Flume died, terminating."
		kill $PID_FEED
		break
	fi
	sleep 1
done
