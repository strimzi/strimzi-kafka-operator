#!/bin/sh
set -x
JAR=$1
shift

CEKIT_DYNAMIC_REOSURCES="/usr/local/dynamic-resources/dynamic_resources.sh"
if [ -f $CEKIT_DYNAMIC_RESOURCES ]; then
  ################################
  # cekit integration settings
  ################################
  source $CEKIT_DYNAIMC_RESOURCES

  JAVA_OPTS="$(/opt/run-java/java-default-options)"
else 
  ################################
  # standalone docker settings
  ################################
  source /bin/dynamic_resources.sh

  MAX_HEAP=`get_heap_size`
  if [ -n "$MAX_HEAP" ]; then
    JAVA_OPTS="-Xms${MAX_HEAP}m -Xmx${MAX_HEAP}m $JAVA_OPTS"
  fi

  export MALLOC_ARENA_MAX=2

  # Enable GC logging for memory tracking
  JAVA_OPTS="${JAVA_OPTS} -XX:NativeMemoryTracking=summary -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps"
fi
  
# Make sure that we use /dev/urandom
JAVA_OPTS="${JAVA_OPTS} -Dvertx.cacheDirBase=/tmp -Djava.security.egd=file:/dev/./urandom"

exec java $JAVA_OPTS -jar $JAR $JAVA_OPTS $@
