#!/usr/bin/env bash
set -e
set +x

# expand gc options based upon java version
function get_gc_opts {
  if [ "${STRIMZI_GC_LOG_ENABLED}" == "true" ]; then
    echo "-Xlog:gc*:stdout:time -XX:NativeMemoryTracking=summary"
  else
    # no gc options
    echo ""
  fi
}

export MALLOC_ARENA_MAX=2

# Make sure that we use /dev/urandom
JAVA_OPTS="${JAVA_OPTS} -Dvertx.cacheDirBase=/tmp/vertx-cache -Djava.security.egd=file:/dev/./urandom"

# Enable GC logging for memory tracking
JAVA_OPTS="${JAVA_OPTS} $(get_gc_opts)"

# Deny illegal access option is supported only on Java 9 and higher
JAVA_OPTS="${JAVA_OPTS} --illegal-access=deny"

# Default memory options used when the user didn't configured any of these options, we set the defaults
if [[ "$JAVA_OPTS" != *"MinRAMPercentage"* && "$JAVA_OPTS" != *"MaxRAMPercentage"* && "$JAVA_OPTS" != *"InitialRAMPercentage"* ]]; then
  JAVA_OPTS="${JAVA_OPTS} -XX:MinRAMPercentage=10 -XX:MaxRAMPercentage=20 -XX:InitialRAMPercentage=10"
fi

# Disable FIPS if needed
if [ "$FIPS_MODE" = "disabled" ]; then
    JAVA_OPTS="${JAVA_OPTS} -Dcom.redhat.fips=false"
fi

set -x

# shellcheck disable=SC2086
exec /usr/bin/tini -w -e 143 -- java $JAVA_OPTS -classpath "$JAVA_CLASSPATH" "$JAVA_MAIN" "$@"
