#!/usr/bin/env bash
set -e

if [ -z "$1" ]; then
    echo "No kafka bootstrap has been given"
    exit 1
else
    export KAFKA_HEADLESS_SERVICE="$1"
fi

if [ -z "$2" ]; then
  echo "No kafka bootstrap port given"
else
    export KAFKA_METRICS_PORT="$2"
fi

if nc -z "$KAFKA_HEADLESS_SERVICE" "$KAFKA_METRICS_PORT"; then
    echo "Couldn't connect to $KAFKA_HEADLESS_SERVICE:$KAFKA_METRICS_PORT"
    exit 1
fi