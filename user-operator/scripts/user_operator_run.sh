#!/usr/bin/env bash
if [ -f /opt/user-operator/custom-config/log4j2.properties ];
then
    export JAVA_OPTS="${JAVA_OPTS} -Dlog4j2.configurationFile=file:/opt/user-operator/custom-config/log4j2.properties"
fi

if [ -n "$STRIMZI_JAVA_SYSTEM_PROPERTIES" ]; then
    export JAVA_OPTS="${JAVA_OPTS} ${STRIMZI_JAVA_SYSTEM_PROPERTIES}"
fi

if [ -n "$STRIMZI_HEAP_OPTS" ]; then
    export JAVA_OPTS="${JAVA_OPTS} ${STRIMZI_HEAP_OPTS}"
fi

if [ -n "$STRIMZI_JVM_PERFORMANCE_OPTS" ]; then
    export JAVA_OPTS="${JAVA_OPTS} ${STRIMZI_JVM_PERFORMANCE_OPTS}"
fi

export JAVA_CLASSPATH=lib/io.strimzi.@project.build.finalName@.@project.packaging@:@project.dist.classpath@
export JAVA_MAIN=io.strimzi.operator.user.Main
exec ${STRIMZI_HOME}/bin/launch_java.sh
