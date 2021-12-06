#!/usr/bin/env bash
set -e

# expand gc options based upon java version
function get_gc_opts {
  # The first segment of the version number is '1' for releases < 9; then '9', '10', '11', ...
  echo "-Xlog:gc*:stdout:time"
}

if [ "${STRIMZI_KAFKA_GC_LOG_ENABLED}" == "true" ]; then
    KAFKA_GC_LOG_OPTS=$(get_gc_opts)
    export KAFKA_GC_LOG_OPTS
else
    export KAFKA_GC_LOG_OPTS=" "
    export GC_LOG_ENABLED="false"
fi