#!/usr/bin/env bash

GRACE_PERIOD=$1
TIMER="0"

# Build the regular expression
ZOOKEEPER_ID=$(hostname | awk -F'-' '{print $NF+1}')
PORT_SUFFIX=$((ZOOKEEPER_ID - 1))
EXPRESSION="0.0.0.0\:2181${PORT_SUFFIX}|127.0.0.1\:2888${PORT_SUFFIX}|127.0.0.1\:3888${PORT_SUFFIX}"

while [ $TIMER -lt $GRACE_PERIOD ]; do
  TIMER=$((TIMER + 1))
  LINES=$(netstat -ant | grep -wE "(${EXPRESSION})" | grep LISTEN | wc -l)

  if [ "$LINES" -eq "0" ]; then
    break
  fi

  sleep 1
done

exit 0