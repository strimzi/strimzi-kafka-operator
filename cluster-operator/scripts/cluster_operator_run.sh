#!/usr/bin/env bash
export JAVA_CLASSPATH=lib/io.strimzi.@project.build.finalName@.@project.packaging@:@project.dist.classpath@
export JAVA_MAIN=io.strimzi.operator.cluster.Main

export JAVA_OPTS="${JAVA_OPTS} -Dlog4j2.configurationFile=file:/opt/strimzi/custom-config/log4j2.properties"
exec "${STRIMZI_HOME}/bin/launch_java.sh"