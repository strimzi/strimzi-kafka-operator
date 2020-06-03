#!/usr/bin/env bash
set -e

echo "Waiting for the TLS sidecar to get ready"

while true; do
  if netstat -ntl | grep -q :2181; then
    echo "TLS sidecar should be ready"
    break
  fi

  echo "TLS sidecar is not ready yet, waiting for another 1 second"

  sleep 1
done

exit 0
