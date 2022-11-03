#!/usr/bin/env bash
set -e

# Test broker liveness
if [ -f /var/opt/kafka/kafka-ready ] ; then
  # Test listening on replication port 9091
    netstat -lnt | grep -Eq 'tcp6?[[:space:]]+[0-9]+[[:space:]]+[0-9]+[[:space:]]+[^ ]+:9091.*LISTEN[[:space:]]*'
fi

# Test controller liveness
. ./kafka_controller_liveness_readiness.sh
