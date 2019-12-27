#!/usr/bin/env bash

echo "Waiting for the TLS sidecar to get ready"

while true; do
  netstat -ntl | grep -q :2181
  RESULT=$?

  if [ "$RESULT" -eq "0" ]; then
    break
  fi

  echo "TLS sidecar is not ready yet, waiting for another 1 second"

  sleep 1
done

exit 0
