#!/usr/bin/env bash
set -e

GRACE_PERIOD=$1
TIMER="0"

while [ $TIMER -lt "$GRACE_PERIOD" ]; do
  TIMER=$((TIMER + 1))
  CONNS=$(netstat -ant | grep -w 127.0.0.1:2181 | grep -c ESTABLISHED)

  if [ "$CONNS" -eq "0" ]; then
    break
  fi

  sleep 1
done

exit 0