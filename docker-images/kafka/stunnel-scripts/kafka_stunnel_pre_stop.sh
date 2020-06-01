#!/usr/bin/env bash

while true; do
  CONNS=$(netstat -ant | grep -w 127.0.0.1:2181 | grep -c ESTABLISHED)
  LISTENERS=$(netstat -ant | grep -w :9091 | grep -c LISTEN)

  if [ "$CONNS" -eq "0" ] && [ "$LISTENERS" -eq "0" ]; then
    break
  fi

  sleep 1
done

exit 0
