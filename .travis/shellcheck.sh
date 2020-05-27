#!/usr/bin/env bash
set -e

SCRIPT_DIRS="docker-images/operator/scripts/*.sh
docker-images/operator/tmp/bin/*.sh
docker-images/kafka/scripts/*.sh
docker-images/kafka/cruise-control-scripts/*.sh
docker-images/kafka/stunnel-scripts/*.sh
docker-images/kafka/exporter-scripts/*.sh
docker-images/kafka/s2i-scripts/*
docker-images/jmxtrans/*.sh"

for SCRIPTS in $SCRIPT_DIRS; do
    shellcheck -a -P $(dirname "$SCRIPTS") -x "$SCRIPTS"
done