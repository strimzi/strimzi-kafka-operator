#!/usr/bin/env bash
if [ -f /tmp/kafka-ready ] ; then
  rm /tmp/zk-connected 2&> /dev/null
  # Test listening on replication port 9091
  netstat -lnt | grep -Eq 'tcp6?[[:space:]]+[0-9]+[[:space:]]+[0-9]+[[:space:]]+[^ ]+:9091.*LISTEN[[:space:]]*'
else
  # Not yet ready, so test ZK connected state
  test -f /tmp/zk-connected
fi
