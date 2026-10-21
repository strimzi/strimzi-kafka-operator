#!/usr/bin/env bash

set -e
set +x

# Clean-up /tmp directory from files which might have remained from previous container restart
# We ignore any errors which might be caused by files injected by different agents which we do not have the rights to delete
rm -rfv /tmp/* || true

# Find my path to use when calling scripts
MYPATH="$(dirname "$0")"

# Configure heap based on the available resources if needed (script is shared with the Kafka image and runs from KAFKA_HOME)
. ./dynamic_resources.sh
if [ -n "$KAFKA_HEAP_OPTS" ]; then
    JAVA_OPTS="${JAVA_OPTS} ${KAFKA_HEAP_OPTS}"
fi

if [ -n "$KAFKA_JVM_PERFORMANCE_OPTS" ]; then
    JAVA_OPTS="${JAVA_OPTS} ${KAFKA_JVM_PERFORMANCE_OPTS}"
fi

JAVA_OPTS="${JAVA_OPTS} -XX:+ExitOnOutOfMemoryError"

if [ "$FIPS_MODE" = "disabled" ]; then
    export JAVA_OPTS="${JAVA_OPTS} -Dcom.redhat.fips=false"
fi

if [ -n "$STRIMZI_JAVA_SYSTEM_PROPERTIES" ]; then
    export JAVA_OPTS="${JAVA_OPTS} ${STRIMZI_JAVA_SYSTEM_PROPERTIES}"
fi

if [ -f /opt/sova/custom-config/log4j2.properties ]; then
    export JAVA_OPTS="${JAVA_OPTS} -Dlog4j2.configurationFile=file:/opt/sova/custom-config/log4j2.properties"
fi

echo "Starting Sova with JAVA_OPTS: ${JAVA_OPTS}"

exec /usr/bin/tini -s -w -e 143 -- java $JAVA_OPTS -classpath "${MYPATH}/../libs/*" io.strimzi.sova.Main "$@"
