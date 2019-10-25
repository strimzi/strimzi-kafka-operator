#!/usr/bin/env bash

# Build the regular expression
ZOOKEEPER_ID=$(hostname | awk -F'-' '{print $NF+1}')
PORT_SUFFIX=$((ZOOKEEPER_ID - 1))
EXPRESSION="0.0.0.0\:2181${PORT_SUFFIX}|127.0.0.1\:2888${PORT_SUFFIX}|127.0.0.1\:3888${PORT_SUFFIX}"

while true; do
  LINES=$(netstat -ant | grep -wE "(${EXPRESSION})" | grep LISTEN | wc -l)

  if [ "$LINES" -eq "0" ]; then
    break
  fi

  sleep 1
done

exit 0