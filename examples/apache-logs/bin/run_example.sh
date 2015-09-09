#!/bin/bash
#

cd "$(dirname $0)/../"

APP_NAME="run_example.sh"

LOG_DIR="${LOG_DIR:-logs}"
LOG_FEED_DATA="${LOG_DIR}/feed_data.sh.log"
LOG_RUN_FLUME="${LOG_DIR}/run_flume.sh.log"

BIN_FEED_DATA=bin/feed_data.sh
BIN_RUN_FLUME=bin/run_flume.sh

DB=${DB:-elasticsearch}
FLUME_CONF_PROPERTIES=conf/flume-conf.properties
DEFAULT_FLUME_CONF_PROPERTIES=conf/flume-conf-${DB}.properties



log() {
	logger -s -t "${APP_NAME}" -i $@
}

cleanup() {
	log "SIGINT catched, trying to shutdown gracefully..."
	log "Killing ${BIN_FEED_DATA} ($PID_FEED)..."
	kill $PID_FEED
	log "Killing ${BIN_RUN_FLUME} ($PID_RUN)..."
	kill $PID_RUN
	sleep 5
	if ps -p $PID_RUN > /dev/null 2>&1 ; then
		log "Killing (-9) ${BIN_RUN_FLUME} ($PID_RUN)..."
		kill -9 $PID_RUN
	fi
	exit $?
}
trap cleanup SIGINT

#if [[ ! -e ${DEFAULT_FLUME_CONF_PROPERTIES} ]] ; then
#	logger "${DEFAULT_FLUME_CONF_PROPERTIES} does not exist!"
#	exit 1
#fi

mkdir -p "${LOG_DIR}"

log "Running feed_data.sh (logging to ${LOG_FEED_DATA})"
"${BIN_FEED_DATA}" &> "${LOG_FEED_DATA}" &
PID_FEED=$!

#log "Copying ${DEFAULT_FLUME_CONF_PROPERTIES} to ${FLUME_CONF_PROPERTIES}."
#cp "${DEFAULT_FLUME_CONF_PROPERTIES}" "${FLUME_CONF_PROPERTIES}"

log "Running run_flume.sh (logging to ${LOG_RUN_FLUME})"
"${BIN_RUN_FLUME}" &> "${LOG_RUN_FLUME}" &
PID_RUN=$!
while true ; do
	if ! ps -p $PID_RUN > /dev/null 2>&1 ; then
		log "Flume died, terminating."
		kill $PID_FEED
		break
	fi
	sleep 1
done
