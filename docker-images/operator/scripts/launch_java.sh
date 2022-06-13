#!/usr/bin/env bash
set -e
set +x

mem_file_cgroups_v2="/sys/fs/cgroup/memory.max"

# expand gc options based upon java version
function get_gc_opts {
  if [ "${STRIMZI_GC_LOG_ENABLED}" == "true" ]; then
    echo "-Xlog:gc*:stdout:time -XX:NativeMemoryTracking=summary"
  else
    # no gc options
    echo ""
  fi
}

calc_maximum_size_opt() {
  local max_mem="$1"
  local percentage="$2"

  local value_in_mb=$((max_mem*percentage/100/1048576))
  echo "-Xmx${value_in_mb}m"
}

# Calculate the value of -Xmx options base on cgroups_v2 values
calc_max_memory() {
  local mem_limit
  mem_limit="$(cat ${mem_file_cgroups_v2})"

   if [ "${mem_limit}" -le 314572800 ]; then
    # Restore the one-fourth default heap size instead of the one-half below 300MB threshold
    # See https://docs.oracle.com/javase/8/docs/technotes/guides/vm/gctuning/parallel.html#default_heap_size
    calc_maximum_size_opt "${mem_limit}" "50"
  else
    calc_maximum_size_opt "${mem_limit}" "75"
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
if [[ ! -r "${mem_file_cgroups_v2}" && "$JAVA_OPTS" != *"MinRAMPercentage"* && "$JAVA_OPTS" != *"MaxRAMPercentage"* && "$JAVA_OPTS" != *"InitialRAMPercentage"* ]]; then
  JAVA_OPTS="${JAVA_OPTS} -XX:MinRAMPercentage=10 -XX:MaxRAMPercentage=20 -XX:InitialRAMPercentage=10"
elif [[ -r "${mem_file_cgroups_v2}" && "$JAVA_OPTS" != *"Xmx"* ]]; then
  # Calculate -Xmx java option
  JAVA_OPTS="${JAVA_OPTS} $(calc_max_memory)"
fi

# Disable FIPS if needed
if [ "$FIPS_MODE" = "disabled" ]; then
    JAVA_OPTS="${JAVA_OPTS} -Dcom.redhat.fips=false"
fi

set -x

# shellcheck disable=SC2086
exec /usr/bin/tini -w -e 143 -- java $JAVA_OPTS -classpath "$JAVA_CLASSPATH" "$JAVA_MAIN" "$@"
