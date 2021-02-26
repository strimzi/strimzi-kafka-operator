#!/usr/bin/env bash
set -e

# Find my path to use when calling scripts
MYPATH="$(dirname "$0")"

# Configure logging
#if [ -z "$LOG4J_OPTS" ]
#then
#      $LOG4J_OPTS="-Dlog4j2.configurationFile=file:${MYPATH}/../config/log4j2.properties"
#fi

# Make sure that we use /dev/urandom
#JAVA_OPTS="${JAVA_OPTS} -Dvertx.cacheDirBase=/tmp -Djava.security.egd=file:/dev/./urandom"

exec java $JAVA_OPTS $LOG4J_OPTS -classpath "${MYPATH}/../libs/*" io.strimzi.kafka.api.conversion.cli.EntryCommand "$@"