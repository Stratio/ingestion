#!/bin/bash

cd "$(dirname $0)/../"

APP_NAME="feed_data.sh"

BASE_URL="feed_data.sh"
DATA_DIR="data"
SPOOL_DIR="${DATA_DIR}/spooldir"
TMP_DIR="${DATA_DIR}/tmp"

URL="ftp://ita.ee.lbl.gov/traces/"
JUL_LOG="NASA_access_log_Jul95"
AUG_LOG="NASA_access_log_Aug95"
GZ=".gz"

log() {
    logger -s -t "${APP_NAME}" -i $@
}

log "Creating SPOOL_DIR $SPOOL_DIR"
mkdir -p "${SPOOL_DIR}"
log "Creating TMP_DIR $TMP_DIR"
mkdir -p "${TMP_DIR}"

wget $URL$JUL_LOG$GZ -O "$TMP_DIR/$JUL_LOG$GZ"
gunzip $TMP_DIR/$JUL_LOG$GZ
mv "$TMP_DIR/$JUL_LOG" "$SPOOL_DIR/$JUL_LOG"
log "Downloaded $JUL_LOG"

wget $URL$AUG_LOG$GZ -O "$TMP_DIR/$AUG_LOG$GZ"
gunzip $TMP_DIR/$AUG_LOG$GZ
mv "$TMP_DIR/$AUG_LOG" "$SPOOL_DIR/$AUG_LOG"
log "Downloaded $AUG_LOG"



