#!/usr/bin/env bash

# expand gc options based upon java version
function get_gc_opts {
  # The first segment of the version number is '1' for releases < 9; then '9', '10', '11', ...
  JAVA_MAJOR_VERSION=$(java -version 2>&1 | sed -E -n 's/.* version "([0-9]*).*$/\1/p')

  GC_LOG_FILE="${STRIMZI_GC_LOG_FILEPATH:=/var/kafka}/diagnostics/gc.log"

  if [ "$JAVA_MAJOR_VERSION" -ge "9" ]; then
    OPTS="-Xlog:gc*:"
    if [ "${STRIMZI_GC_LOG_TO_FILE}" == "true" ]; then
      OPTS+="file=${GC_LOG_FILE}::filecount=5,filesize=40m"
    else
      OPTS+="stdout:time"
    fi
    echo "$OPTS"
  else
    OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps"
    if [ "${STRIMZI_GC_LOG_TO_FILE}" == "true" ]; then
      OPTS+=" -Xloggc:${GC_LOG_FILE} -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=40M -XX:+UseGCLogFileRotation"
    fi
    echo "$OPTS"
  fi
}

if [ "${STRIMZI_KAFKA_GC_LOG_ENABLED}" == "true" ]; then
    export KAFKA_GC_LOG_OPTS=$(get_gc_opts)
else
    export KAFKA_GC_LOG_OPTS=" "
    export GC_LOG_ENABLED="false"
fi