#!/usr/bin/env bash

while true; do
  CONNS=$(netstat -ant | grep -w 127.0.0.1:2181 | grep ESTABLISHED | wc -l)
  LISTENERS=$(netstat -ant | grep -w :9091 | grep LISTEN | wc -l)

  if [ "$CONNS" -eq "0" ] && [ "$LISTENERS" -eq "0" ]; then
    break
  fi

  sleep 1
done

exit 0
