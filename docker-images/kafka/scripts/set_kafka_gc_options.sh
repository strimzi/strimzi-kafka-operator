#!/usr/bin/env bash

# expand gc options based upon java version
function get_gc_opts {
  # The first segment of the version number is '1' for releases < 9; then '9', '10', '11', ...
  JAVA_MAJOR_VERSION=$(java -version 2>&1 | sed -E -n 's/.* version "([0-9]*).*$/\1/p')
  if [ "$JAVA_MAJOR_VERSION" -ge "9" ] ; then
    echo "-Xlog:gc*:stdout:time"
  else
    echo "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps"
  fi
}

if [ "${STRIMZI_KAFKA_GC_LOG_ENABLED}" == "true" ]; then
    export KAFKA_GC_LOG_OPTS=$(get_gc_opts)
else
    export KAFKA_GC_LOG_OPTS=" "
    export GC_LOG_ENABLED="false"
fi