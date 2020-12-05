#!/usr/bin/env bash
set -x
JAR=$1
shift

. /bin/dynamic_resources.sh

# expand gc options based upon java version
function get_gc_opts {
  if [ "${STRIMZI_GC_LOG_ENABLED}" == "true" ]; then
    echo "-Xlog:gc*:stdout:time -XX:NativeMemoryTracking=summary"
  else
    # no gc options
    echo ""
  fi
}

MAX_HEAP=`get_heap_size`
if [ -n "$MAX_HEAP" ]; then
  JAVA_OPTS="-Xms${MAX_HEAP}m -Xmx${MAX_HEAP}m $JAVA_OPTS"
fi

export MALLOC_ARENA_MAX=2

# Make sure that we use /dev/urandom
JAVA_OPTS="${JAVA_OPTS} -Dvertx.cacheDirBase=/tmp -Djava.security.egd=file:/dev/./urandom"

# Enable GC logging for memory tracking
JAVA_OPTS="${JAVA_OPTS} $(get_gc_opts)"

# Deny illegal access option is supported only on Java 9 and higher
JAVA_OPTS="${JAVA_OPTS} --illegal-access=deny"

exec java $JAVA_OPTS -jar $JAR $@
