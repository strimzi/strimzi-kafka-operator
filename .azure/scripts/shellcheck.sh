#!/usr/bin/env bash
set -e

SCRIPT_DIRS="docker-images/operator/scripts/*.sh
cluster-operator/scripts/*.sh
user-operator/scripts/*.sh
topic-operator/scripts/*.sh
kafka-init/scripts/*.sh
docker-images/kafka-based/kafka/scripts/*.sh
docker-images/kafka-based/kafka/cruise-control-scripts/*.sh
docker-images/kafka-based/kafka/exporter-scripts/*.sh
tools/*.sh"

for SCRIPTS in $SCRIPT_DIRS; do
    shellcheck -a -P $(dirname "$SCRIPTS") -x "$SCRIPTS"
done
