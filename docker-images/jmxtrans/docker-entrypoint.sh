#!/usr/bin/env bash
set -e

EXEC="-jar $JAR_FILE -e -j $JSON_DIR -s $SECONDS_BETWEEN_RUNS -c $CONTINUE_ON_ERROR $ADDITIONAL_JARS_OPTS"
GC_OPTS="-Xms${HEAP_SIZE}m -Xmx${HEAP_SIZE}m -XX:PermSize=${PERM_SIZE}m -XX:MaxPermSize=${MAX_PERM_SIZE}m"
JMXTRANS_OPTS="$JMXTRANS_OPTS -Dlog4j2.configurationFile=file:///${JMXTRANS_HOME}/conf/log4j2.properties"

if [ -n "${KAFKA_JMX_USERNAME}" ]; then
  JMXTRANS_OPTS="$JMXTRANS_OPTS -Dkafka.username=${KAFKA_JMX_USERNAME} -Dkafka.password=${KAFKA_JMX_PASSWORD}"
fi

MONITOR_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false \
              -Dcom.sun.management.jmxremote.authenticate=false \
              -Dcom.sun.management.jmxremote.port=9999 \
              -Dcom.sun.management.jmxremote.rmi.port=9999 \
              -Djava.rmi.server.hostname=${PROXY_HOST}"

if [ "$1" = 'start-without-jmx' ]; then
    # shellcheck disable=SC2086
    set /usr/bin/tini -w -e 143 -- java -server $JAVA_OPTS $JMXTRANS_OPTS $GC_OPTS $EXEC
elif [ "$1" = 'start-with-jmx' ]; then
    # shellcheck disable=SC2086
    set /usr/bin/tini -w -e 143 -- java -server $JAVA_OPTS $JMXTRANS_OPTS $GC_OPTS $MONITOR_OPTS $EXEC
fi

exec "$@"
