#!/usr/bin/env bash
export JAVA_CLASSPATH=${TP_CLASSPATH}
export JAVA_MAIN=${TP_MAIN}
exec ${STRIMZI_HOME}/bin/launch_java.sh