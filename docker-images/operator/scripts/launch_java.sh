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

# This functions calculates the max memory that the container could using cgroups V2
max_memory_using_cgroups_v2() {
  # High number which is the max limit until which memory is supposed to be
  # unbounded.
  local max_mem_cgroup=0
  local max_mem_meminfo_kb
  max_mem_meminfo_kb="$(< /proc/meminfo awk '/MemTotal/ {print $2}')"
  local max_mem_meminfo
  max_mem_meminfo=$((max_mem_meminfo_kb*1024))

   max_mem_cgroup="$(cat ${mem_file_cgroups_v2})"

  if [ "${max_mem_cgroup:-0}" != -1 ] && [ "${max_mem_cgroup:-0}" -lt ${max_mem_meminfo:-0} ]; then
    echo "${max_mem_cgroup}"
  fi
}

# set an enviroment variable CONTAINER_MAX_MEMORY with the memory limit of the container
init_memory_limit_env_vars() {
  # Read in container limits and export the as environment variables
  local mem_limit
  mem_limit="$(max_memory_using_cgroups_v2)"
  if [ -n "${mem_limit}" ]; then
    export CONTAINER_MAX_MEMORY="${mem_limit}"
  fi
}

calc_mem_opt() {
  local max_mem="$1"
  local fraction="$2"
  local mem_opt="$3"

  local val
  val=$(calc "round($1*$2/100/1048576)" "${max_mem}" "${fraction}")
  echo "-X${mem_opt}${val}m"
}

# Generic formula evaluation based on awk
calc() {
  local formula="$1"
  shift
  echo "$@" | awk '
    function round(x) {
      return int(x + 0.5)
    }
    {print '"int(${formula})"'}
  '
}

# Calculate the value of -Xmx options base on CONTAINER_MAX_MEMORY
calc_max_memory() {
  if [ -z "${CONTAINER_MAX_MEMORY:-}" ]; then
    return
  fi

  if [ "${CONTAINER_MAX_MEMORY}" -le 314572800 ]; then
    # Restore the one-fourth default heap size instead of the one-half below 300MB threshold
    # See https://docs.oracle.com/javase/8/docs/technotes/guides/vm/gctuning/parallel.html#default_heap_size
    calc_mem_opt "${CONTAINER_MAX_MEMORY}" "50" "mx"
  else
    calc_mem_opt "${CONTAINER_MAX_MEMORY}" "75" "mx"
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
  # Set env vars reflecting memory limits
  init_memory_limit_env_vars
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
