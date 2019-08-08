#!/usr/bin/env bash

function get_heap_size {
  CONTAINER_MEMORY_IN_BYTES=`cat /sys/fs/cgroup/memory/memory.limit_in_bytes`
   # use max of 31G memory, java performs much better with Compressed Ordinary Object Pointers
  DEFAULT_MEMORY_CEILING=$((31 * 2**20))
  if [ "${CONTAINER_MEMORY_IN_BYTES}" -lt "${DEFAULT_MEMORY_CEILING}" ]; then
    if [ -z $CONTAINER_HEAP_PERCENT ]; then
      CONTAINER_HEAP_PERCENT=0.50
    fi

    CONTAINER_MEMORY_IN_MB=$((${CONTAINER_MEMORY_IN_BYTES}/1024**2))
    CONTAINER_HEAP_MAX=$(echo "${CONTAINER_MEMORY_IN_MB} ${CONTAINER_HEAP_PERCENT}" | awk '{ printf "%d", $1 * $2 }')

    echo "${CONTAINER_HEAP_MAX}"
  fi
}
