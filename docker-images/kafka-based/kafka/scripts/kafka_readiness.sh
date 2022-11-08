#!/usr/bin/env bash
set -e

# Test ZK-based broker and KRaft broker readiness
test -f /var/opt/kafka/kafka-ready

if [ "$STRIMZI_KRAFT_ENABLED" = "true" ]; then
  # Test KRaft controller readiness
  . ./kafka_controller_liveness_readiness.sh
fi
