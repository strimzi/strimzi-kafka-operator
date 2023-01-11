#!/usr/bin/env bash
set -e

if [ "$STRIMZI_KRAFT_ENABLED" = "true" ]; then
  # Test KRaft controller process is running
  . ./kafka_controller_liveness.sh
else
  # Test ZK-based broker liveness
  # We expect that either the broker is ready and listening on 9091 (replication port)
  # or it has a ZK session
  if [ -f /var/opt/kafka/kafka-ready ] ; then
    rm -f /var/opt/kafka/zk-connected 2&> /dev/null
    # Test listening on replication port 9091
    netstat -lnt | grep -Eq 'tcp6?[[:space:]]+[0-9]+[[:space:]]+[0-9]+[[:space:]]+[^ ]+:9091.*LISTEN[[:space:]]*'
  else
    # Not yet ready, so test ZK connected state
    test -f /var/opt/kafka/zk-connected
  fi
fi
