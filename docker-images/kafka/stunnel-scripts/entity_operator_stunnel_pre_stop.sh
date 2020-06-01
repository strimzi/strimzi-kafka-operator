#!/usr/bin/env bash
set -e

while true; do
  CONNS=$(netstat -ant | grep -w 127.0.0.1:2181 | grep -c ESTABLISHED)

  if [ "$CONNS" -eq "0" ]; then
    break
  fi

  sleep 1
done

exit 0