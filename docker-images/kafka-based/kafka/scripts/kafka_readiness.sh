#!/usr/bin/env bash
set -e

if [ "$STRIMZI_KRAFT_ENABLED" = "true" ]; then
  # Test KRaft controller process is running
  # This is the best check we can do in a combined node until KafkaRoller is updated
  # See proposal 046 for more details
  . ./kafka_controller_liveness.sh
else
  # Test ZK-based broker readiness
  # The kafka-agent will create /var/opt/kafka/kafka-ready in the container when the broker
  # state is >= 3 && != 127 (UNKNOWN state)
  test -f /var/opt/kafka/kafka-ready
fi
