#!/usr/bin/env bash
set -e

source ./kraft_utils.sh
USE_KRAFT=$(useKRaft)

if [ "$USE_KRAFT" == "true" ]; then
  # Test KRaft broker/controller readiness
  . ./kafka_readiness_kraft.sh
else
  # Test ZK-based broker readiness
  # The kafka-agent will create /var/opt/kafka/kafka-ready in the container when the broker
  # state is >= 3 && != 127 (UNKNOWN state)
  test -f /var/opt/kafka/kafka-ready
fi
