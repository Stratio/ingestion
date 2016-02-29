#!/bin/bash
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# Get options
while getopts "c:f:n:p:" option; do
   case $option in
      c)      CONF=$OPTARG ;;
      f)      CONF_FILE=$OPTARG ;;
      n)      FLUME_NAME=$OPTARG ;;
      p)      PIDFILE=$OPTARG ;;
      *)      echo "Unknown option" ; exit 1 ;;
   esac
done

$DIR/flume-ng agent --conf $CONF --conf-file $CONF_FILE --name $FLUME_NAME >/dev/null 2>&1 & echo $! >$PIDFILE
