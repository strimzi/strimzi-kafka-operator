#!/usr/bin/env bash
set -e

function get_heap_size {
  PERCENTAGE=$1
  MAX=$2
  # Get the max heap used by a jvm which used all the ram available to the container
  POSSIBLE_HEAP=$(java -XX:MaxRAMPercentage="$PERCENTAGE" -XshowSettings:vm -version \
    |& awk '/Max\. Heap Size \(Estimated\): [0-9KMG]+/{ print $5}' \
    | gawk -f to_bytes.gawk)

  if [ "${MAX}" ]; then
    MAX=$(echo "${MAX}" | gawk -f to_bytes.gawk)
    if [ "${MAX}" -lt "${POSSIBLE_HEAP}" ]; then
      POSSIBLE_HEAP=$MAX
    fi
  fi

  echo "$POSSIBLE_HEAP"
}
