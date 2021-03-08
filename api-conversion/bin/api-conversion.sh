#!/usr/bin/env bash
set -e

# Find my path to use when calling scripts
MYPATH="$(dirname "$0")"

exec java -classpath "${MYPATH}/../libs/*" io.strimzi.kafka.api.conversion.cli.EntryCommand "$@"