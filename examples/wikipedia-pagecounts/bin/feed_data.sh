#!/bin/bash

cd "$(dirname $0)/../"

APP_NAME="feed_data.sh"

BASE_URL="http://dumps.wikimedia.org/other/pagecounts-raw/"
DATA_DIR="data"
SPOOL_DIR="${DATA_DIR}/spooldir"
TMP_DIR="${DATA_DIR}/tmp"

YEAR_START=${YEAR_START:-2007}
YEAR_END=${YEAR_END:-$(date +%Y)}
MONTH_START=${MONTH_START:-1}
MONTH_END=${MONTH_END:-12}
FILTER_LANG=${FILTER_LANG:-en}

MAX_FEED=${MAX_FEED:-9999999}
ADDED_FILES=0

mkdir -p "${SPOOL_DIR}"
mkdir -p "${TMP_DIR}"

log() {
	logger -s -t "${APP_NAME}" -i $@
}

for YEAR in $(seq $YEAR_START $YEAR_END) ; do
	for MONTH in $(seq $MONTH_START $MONTH_END) ; do
		if [[ $(echo $MONTH | wc -c) = 2 ]] ; then
			MONTH=0$MONTH
		fi
		log "Fetching file list for year $YEAR month $MONTH"
		FILES=$(wget ${BASE_URL}/${YEAR}/${YEAR}-${MONTH}/ -O - -q | grep 'a href="pagecounts.*gz' | sed -e 's:.*href="\(pagecounts-[0-9]*-[0-9]*.gz\)\".*:\1:g')
                #TODO: Support recursive spooldir https://issues.apache.org/jira/browse/FLUME-1899
		#TODO: Implement proper Flume decompress support
		for FILE in $FILES ; do
			if [[ $ADDED_FILES -ge $MAX_FEED ]] ; then
				log "Already fetched $MAX_FEED (max), exiting."
				exit 0
			fi
			ADDED_FILES=$((ADDED_FILES + 1))
			UNCOMPRESSED_FILE="${FILE/.gz/}"
			if [[ -e $SPOOL_DIR/$UNCOMPRESSED_FILE ]] ; then
				log "FILE already exists and it is queued"
			elif [[ -e $SPOOL_DIR/${UNCOMPRESSED_FILE}.COMPLETED ]] ; then
				log "$FILE is already processed"
			else
				log "$FILE is being downloaded and queued"
				wget "$BASE_URL/$YEAR/$YEAR-$MONTH/$FILE" -O "$TMP_DIR/$FILE" -q
				if [[ $? != 0 ]] ; then
					log "ERROR wget"
					exit 1
				fi
				#gunzip "${TMP_DIR}/${FILE}"
				#mv "$TMP_DIR/$UNCOMPRESSED_FILE" "$SPOOL_DIR/$UNCOMPRESSED_FILE"
				zcat "${TMP_DIR}/${FILE}" | grep ^${FILTER_LANG} | grep -v "1 1$" > "$TMP_DIR/$UNCOMPRESSED_FILE"
				mv "$TMP_DIR/$UNCOMPRESSED_FILE" "$SPOOL_DIR/$UNCOMPRESSED_FILE"
			fi
		done
	done
done
