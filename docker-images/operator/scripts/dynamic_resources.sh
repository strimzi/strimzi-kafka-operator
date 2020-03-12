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

function set_xmx_xms {
  MAX_HEAP=`get_heap_size`
  if [ -n "$MAX_HEAP" ]; then
      if [[ $STRIMZI_JAVA_SYSTEM_PROPERTIES == *"-Xmx"* ]]; then
          XMX_STRING=$(echo $STRIMZI_JAVA_SYSTEM_PROPERTIES | sed -n 's/.*\(-Xmx[0-9]\+.\).*/\1/p')
          XMX_MBYTES=$(($(echo $XMX_STRING | cut -c 5- | gawk -f ./bin/to_bytes.gawk)/1024**2))
          if [[ $XMX_MBYTES -lt $MAX_HEAP ]]; then
             # "Xmx set less than MAX_HEAP, using MAX_HEAP"
             JAVA_OPTS=$(echo $JAVA_OPTS | sed s/$XMX_STRING/-Xmx${MAX_HEAP}m/)
          fi
      fi

      if [[ $STRIMZI_JAVA_SYSTEM_PROPERTIES == *"-Xms"* ]]; then
          XMS_STRING=$(echo $STRIMZI_JAVA_SYSTEM_PROPERTIES | sed -n 's/.*\(-Xms[0-9]\+.\).*/\1/p')
          XMS_MBYTES=$(($(echo $XMS_STRING | cut -c 5- | gawk -f ./bin/to_bytes.gawk)/1024**2))
          if [[ $XMS_MBYTES -gt $MAX_HEAP ]]; then
             # "Xms set greater than MAX_HEAP, using MAX_HEAP"
             JAVA_OPTS=$(echo $JAVA_OPTS | sed s/$XMS_STRING/-Xms${MAX_HEAP}m/)
          fi
      fi
  fi
}