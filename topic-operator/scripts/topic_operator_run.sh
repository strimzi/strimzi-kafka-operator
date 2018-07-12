#!/bin/bash
set +x

if [ -f /opt/topic-operator/custom-config/log4j.properties ];
then
    export JAVA_OPTS="${JAVA_OPTS} -Dlog4j.configurationFile=file:/opt/topic-operator/custom-config/log4j.properties"
fi

exec /bin/launch_java.sh $1
