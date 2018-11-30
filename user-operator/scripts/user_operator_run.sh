#!/bin/bash

if [ -f /opt/user-operator/custom-config/log4j2.properties ];
then
    export JAVA_OPTS="${JAVA_OPTS} -Dlog4j2.configurationFile=file:/opt/user-operator/custom-config/log4j2.properties"
fi

exec /bin/launch_java.sh $1
