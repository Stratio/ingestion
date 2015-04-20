#!/bin/bash

cd "$(dirname $0)/../"

APP_NAME="feed_data.sh"

BASE_URL="feed_data.sh"
DATA_DIR="data"
SPOOL_DIR="${DATA_DIR}/spooldir"
TMP_DIR="${DATA_DIR}/tmp"

YEAR_START=1987
YEAR_END=2008

log() {
    logger -s -t "${APP_NAME}" -i $@
}

log "Creating SPOOL_DIR $SPOOL_DIR"
mkdir -p "${SPOOL_DIR}"
log "Creating TMP_DIR $TMP_DIR"
mkdir -p "${TMP_DIR}"

for YEAR in $(seq $YEAR_START $YEAR_END); do
    FILE=${YEAR}.csv.bz2
    UNCOMPRESSED_FILE=${YEAR}.csv
    wget "http://stat-computing.org/dataexpo/2009/${YEAR}.csv.bz2" -O "$TMP_DIR/${FILE}"
    log "Downloaded $FILE"

    if [[ -e $SPOOL_DIR/$UNCOMPRESSED_FILE ]] ; then
        log "$FILE already exists and it is queued"
    elif [[ -e $SPOOL_DIR/${UNCOMPRESSED_FILE}.COMPLETED ]] ; then
        log "$FILE is already processed"
    fi
    bunzip2 "${TMP_DIR}/${FILE}"
    log "Uncompressed file in $TMP_DIR/$UNCOMPRESSED_FILE"
    mv "$TMP_DIR/$UNCOMPRESSED_FILE" "$SPOOL_DIR/$UNCOMPRESSED_FILE"
done

