#!/usr/bin/env bash
set -e

function get_heap_size {
  PERCENTAGE=$1
  MAX=$2
  # Get the max heap used by a jvm which used all the ram available to the container
  POSSIBLE_HEAP=$(java -XX:MaxRAMPercentage="$PERCENTAGE" -XshowSettings:vm -version \
    |& awk '/Max\. Heap Size \(Estimated\): [0-9KMGT]+/{ print $5}' \
    | awk -f to_bytes.awk)

  if [ -n "${POSSIBLE_HEAP}" ] && [ "${MAX}" ]; then
    MAX=$(echo "${MAX}" | awk -f to_bytes.awk)
    if [ -n "${MAX}" ] && [ "${MAX}" -lt "${POSSIBLE_HEAP}" ]; then
      POSSIBLE_HEAP=$MAX
    fi
  fi

  echo "$POSSIBLE_HEAP"
}

if [ -z "$KAFKA_HEAP_OPTS" ] && [ -n "${STRIMZI_DYNAMIC_HEAP_PERCENTAGE}" ]; then
    # Calculate a max heap size based some STRIMZI_DYNAMIC_HEAP_PERCENTAGE of the heap
    # available to a jvm using 100% of the CGroup-aware memory
    # up to some optional STRIMZI_DYNAMIC_HEAP_MAX
    CALC_MAX_HEAP=$(get_heap_size "${STRIMZI_DYNAMIC_HEAP_PERCENTAGE}" "${STRIMZI_DYNAMIC_HEAP_MAX}")
    if [ -n "$CALC_MAX_HEAP" ]; then
      export KAFKA_HEAP_OPTS="-Xms${CALC_MAX_HEAP} -Xmx${CALC_MAX_HEAP}"
      echo "Configuring Java heap: $KAFKA_HEAP_OPTS"
    else
      echo "ERROR: Failed to calculate the Java heap size (STRIMZI_DYNAMIC_HEAP_PERCENTAGE=${STRIMZI_DYNAMIC_HEAP_PERCENTAGE}, STRIMZI_DYNAMIC_HEAP_MAX=${STRIMZI_DYNAMIC_HEAP_MAX:-<unset>}). KAFKA_HEAP_OPTS will not be set and the JVM will start with the launch script's default heap, which may not match the container memory limit." >&2
    fi
fi
