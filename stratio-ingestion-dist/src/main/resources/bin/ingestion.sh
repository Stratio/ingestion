#!/bin/bash -x
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# Get options
while getopts "c:f:n:p:l:j:" option; do
   case $option in
      c)      CONF=$OPTARG ;;
      f)      CONF_FILE=$OPTARG ;;
      n)      FLUME_NAME=$OPTARG ;;
      p)      PIDFILE=$OPTARG ;;
      l)      LOG_LEVEL=$OPTARG ;;
      j)      LOG4J_FILE=$OPTARG ;;
      *)      echo "Unknown option" ; exit 1 ;;
   esac
done

$DIR/flume-ng agent --conf $CONF --conf-file $CONF_FILE --name $FLUME_NAME -Dlog4j.configuration=file://$LOG4J_FILE -Dflume.root.logger=$LOG_LEVEL >/dev/null 2>&1 & echo $! >$PIDFILE
