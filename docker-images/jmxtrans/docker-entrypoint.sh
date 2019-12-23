#!/usr/bin/env bash
#
# The MIT License
# Copyright © 2010 JmxTrans team
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#

set -e

EXEC="-jar $JAR_FILE -e -j $JSON_DIR -s $SECONDS_BETWEEN_RUNS -c $CONTINUE_ON_ERROR $ADDITIONAL_JARS_OPTS"
GC_OPTS="-Xms${HEAP_SIZE}m -Xmx${HEAP_SIZE}m -XX:PermSize=${PERM_SIZE}m -XX:MaxPermSize=${MAX_PERM_SIZE}m"
JMXTRANS_OPTS="$JMXTRANS_OPTS -Dlogback.configurationFile=file:///${JMXTRANS_HOME}/conf/logback.xml"

if [ -n "${KAFKA_JMX_USERNAME}" ]; then
  JMXTRANS_OPTS="$JMXTRANS_OPTS -Dkafka.username=${KAFKA_JMX_USERNAME} -Dkafka.password=${KAFKA_JMX_PASSWORD}"
fi

MONITOR_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false \
              -Dcom.sun.management.jmxremote.authenticate=false \
              -Dcom.sun.management.jmxremote.port=9999 \
              -Dcom.sun.management.jmxremote.rmi.port=9999 \
              -Djava.rmi.server.hostname=${PROXY_HOST}"

if [ "$1" = 'start-without-jmx' ]; then
    set tini -- java -server $JAVA_OPTS $JMXTRANS_OPTS $GC_OPTS $EXEC
elif [ "$1" = 'start-with-jmx' ]; then
    set tini -- java -server $JAVA_OPTS $JMXTRANS_OPTS $GC_OPTS $MONITOR_OPTS $EXEC
fi

exec "$@"
