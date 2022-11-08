#!/usr/bin/env bash
set -e

# Test ZK-based broker and KRaft broker liveness
if [ -f /var/opt/kafka/kafka-ready ] ; then
  rm -f /var/opt/kafka/zk-connected 2&> /dev/null
  # Test listening on replication port 9091
  netstat -lnt | grep -Eq 'tcp6?[[:space:]]+[0-9]+[[:space:]]+[0-9]+[[:space:]]+[^ ]+:9091.*LISTEN[[:space:]]*'
elif [ "$STRIMZI_KRAFT_ENABLED" != "true" ]; then
  # Not yet ready, so if in ZK mode test ZK connected state
  test -f /var/opt/kafka/zk-connected
fi

if [ "$STRIMZI_KRAFT_ENABLED" = "true" ]; then
  # Test KRaft controller liveness
  . ./kafka_controller_liveness_readiness.sh
fi
