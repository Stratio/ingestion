#!/bin/bash

DIR=$(dirname $0)/../
cd $DIR

DATA_DIR="data/files"
TMP_DIR="data/tmp_files"

YEAR_START=${1:-1987}
YEAR_END=${2:-2008}

mkdir -p $DATA_DIR
mkdir -p $TMP_DIR

function get_filename() {
	year=$1
	echo "${year}.csv.bz2"
}

function get_url() {
	year=$1
	echo "http://stat-computing.org/dataexpo/2009/$(get_filename $year)"
}

for YEAR in $(seq $YEAR_START $YEAR_END) ; do
	echo "YEAR $YEAR"
	CURRENT_DATA_DIR=$DATA_DIR
	mkdir -p $CURRENT_DATA_DIR
	FILE=$(get_filename $YEAR)
	UNCOMPRESSED_FILE=${FILE/.bz2/}
	URL=$(get_url $YEAR) 
	if [[ -e $CURRENT_DATA_DIR/${UNCOMPRESSED_FILE} ]] ; then
		echo "$FILE already exists and it is queued"
	elif [[ -e $CURRENT_DATA_DIR/${UNCOMPRESSED_FILE}.COMPLETED ]] ; then
		echo "$FILE is already processed"
	else
		echo "$FILE is being downloaded and queued"
		wget "$URL" -O $TMP_DIR/$FILE -q
		if [[ $? != 0 ]] ; then
			echo "ERROR"
			exit 1
		fi
		mv $TMP_DIR/$FILE $CURRENT_DATA_DIR/$FILE
		bunzip2 $CURRENT_DATA_DIR/$FILE
	fi
done
