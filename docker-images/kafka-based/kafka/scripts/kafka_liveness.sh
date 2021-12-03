#!/usr/bin/env bash
set -e

if [ -f /var/opt/kafka/kafka-ready ] ; then
  rm -f /var/opt/kafka/zk-connected 2&> /dev/null
  # Test listening on replication port 9091
  netstat -lnt | grep -Eq 'tcp6?[[:space:]]+[0-9]+[[:space:]]+[0-9]+[[:space:]]+[^ ]+:9091.*LISTEN[[:space:]]*'
else
  # Not yet ready, so test ZK connected state
  test -f /var/opt/kafka/zk-connected
fi
