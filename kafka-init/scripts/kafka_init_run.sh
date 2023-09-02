#!/usr/bin/env bash
set -e

export JAVA_CLASSPATH=$JAVA_CLASSPATH:lib/io.strimzi.@project.build.finalName@.@project.packaging@:@project.dist.classpath@
export JAVA_MAIN=io.strimzi.kafka.init.Main
exec "${STRIMZI_HOME}/bin/launch_java.sh"