#!/usr/bin/env bash
export JAVA_CLASSPATH=lib/io.strimzi.@project.build.finalName@.@project.packaging@:@project.dist.classpath@
export JAVA_MAIN=io.strimzi.operator.cluster.Main

if [ -f /opt/strimzi/custom-config/log4j2.properties ]; then
    # if ConfigMap was not mounted and thus this file was not created, use properties file from the classpath
    export JAVA_OPTS="${JAVA_OPTS} -Dlog4j2.configurationFile=file:/opt/strimzi/custom-config/log4j2.properties"
else
    echo "Configuration file log4j2.properties not found. Using default static logging setting. Dynamic updates of logging configuration will not work."
fi
exec "${STRIMZI_HOME}/bin/launch_java.sh"