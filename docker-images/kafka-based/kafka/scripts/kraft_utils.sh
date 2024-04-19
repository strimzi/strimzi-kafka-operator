#!/usr/bin/env bash
set -e

# Function used to determine if the current node is using KRaft in order to get
# the proper readiness and liveness probes in action other than storage formatting.
# It uses the Kafka metadata configuration state together with the roles for this goal.
#
# It returns "true" if the current node is using KRaft, "false" otherwise
#
function useKRaft {
  STRIMZI_KAFKA_METADATA_CONFIG_STATE=$(cat /tmp/kafka/strimzi.kafka.metadata.config.state)

  file=/tmp/strimzi.properties
  test -f $file

  # During migration, the process.roles field can be still not set on broker only nodes
  # so, because grep would fail, the "|| true" operation allows to return empty roles result
  roles=$(grep -Po '(?<=^process.roles=).+' "$file" || true)

  # controller is KRaft since PRE_MIGRATION
  if [[ "$roles" =~ "controller" ]] && [ "$STRIMZI_KAFKA_METADATA_CONFIG_STATE" -ge 1 ]; then
    echo "true"
  # broker is KRaft starting from POST_MIGRATION
  elif [[ "$roles" =~ "broker" ]] && [ "$STRIMZI_KAFKA_METADATA_CONFIG_STATE" -ge 3 ]; then
    echo "true"
  # we should be here in ZK state only or broker before POST_MIGRATION
  else
    echo "false"
  fi
}
