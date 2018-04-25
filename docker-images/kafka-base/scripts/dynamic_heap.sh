#!/bin/sh

function get_heap_size {
  FRACTION=$1
  max=$2
  # Get the max heap used by a jvm which used all the ram available to the container
  max_possible_heap=$(java -XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap -XX:MaxRAMFraction=1 -XshowSettings:vm -version \
    |& awk '/Max\. Heap Size \(Estimated\): [0-9KMG]+/{ print $5}' \
    | gawk -f to_bytes.gawk)

  actual_heap_max=$(echo "${max_possible_heap} ${fraction}" | awk '{ printf "%d", $1 * $2 }')

  if [ ${max} ]; then
    max=$(echo ${max} | gawk -f to_bytes.gawk)
    if [ ${max} -lt ${actual_heap_max} ]; then
      actual_heap_max=$max
    fi
  fi
  echo $actual_heap_max
}
