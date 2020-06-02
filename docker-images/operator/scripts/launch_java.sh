#!/usr/bin/env bash
set -e
set -x

# expand gc options based upon java version
function get_gc_opts {
  if [ "${STRIMZI_GC_LOG_ENABLED}" == "true" ]; then
    # The first segment of the version number is '1' for releases < 9; then '9', '10', '11', ...
    JAVA_MAJOR_VERSION=$(java -version 2>&1 | sed -E -n 's/.* version "([0-9]*).*$/\1/p')
    if [ "$JAVA_MAJOR_VERSION" -ge "9" ] ; then
      echo "-Xlog:gc*:stdout:time -XX:NativeMemoryTracking=summary"
    else
      echo "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:NativeMemoryTracking=summary"
    fi
  else
    # no gc options
    echo ""
  fi
}

export MALLOC_ARENA_MAX=2

# Make sure that we use /dev/urandom
JAVA_OPTS="${JAVA_OPTS} -Dvertx.cacheDirBase=/tmp -Djava.security.egd=file:/dev/./urandom"

# Enable GC logging for memory tracking
JAVA_OPTS="${JAVA_OPTS} $(get_gc_opts)"

# Deny illegal access option is supported only on Java 9 and higher
JAVA_MAJOR_VERSION=$(java -version 2>&1 | sed -E -n 's/.* version "([0-9]*).*$/\1/p')
if [ "$JAVA_MAJOR_VERSION" -ge "9" ] ; then
  JAVA_OPTS="${JAVA_OPTS} --illegal-access=deny"
fi

# shellcheck disable=SC2086
exec /usr/bin/tini -w -e 143 -- java $JAVA_OPTS -classpath "$JAVA_CLASSPATH" "$JAVA_MAIN" "$@"
