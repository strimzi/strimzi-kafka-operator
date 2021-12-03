#!/usr/bin/env bash
set -e

function get_heap_size {
  FRACTION=$1
  MAX=$2
  # Get the max heap used by a jvm which used all the ram available to the container
  MAX_POSSIBLE_HEAP=$(java -XX:+UnlockExperimentalVMOptions -XX:+UseContainerSupport -XX:MaxRAMFraction=1 -XshowSettings:vm -version \
    |& awk '/Max\. Heap Size \(Estimated\): [0-9KMG]+/{ print $5}' \
    | gawk -f to_bytes.gawk)

  ACTUAL_MAX_HEAP=$(echo "${MAX_POSSIBLE_HEAP} ${FRACTION}" | awk '{ printf "%d", $1 * $2 }')

  if [ "${MAX}" ]; then
    MAX=$(echo "${MAX}" | gawk -f to_bytes.gawk)
    if [ "${MAX}" -lt "${ACTUAL_MAX_HEAP}" ]; then
      ACTUAL_MAX_HEAP=$MAX
    fi
  fi
  echo "$ACTUAL_MAX_HEAP"
}
