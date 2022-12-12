#!/usr/bin/env bash
set -e

# The java.util.logging.manager is set because of OkHttp client which is using JUL logging
export JAVA_OPTS="${JAVA_OPTS} -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager"

export JAVA_CLASSPATH=lib/io.strimzi.@project.build.finalName@.@project.packaging@:@project.dist.classpath@
export JAVA_MAIN=io.strimzi.kafka.init.Main
exec "${STRIMZI_HOME}/bin/launch_java.sh"