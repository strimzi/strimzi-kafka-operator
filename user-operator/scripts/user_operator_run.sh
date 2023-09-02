#!/usr/bin/env bash
set -e

# Clean-up /tmp directory from files which might have remained from previous container restart
# We ignore any errors which might be caused by files injected by different agents which we do not have the rights to delete
rm -rfv /tmp/* || true

if [ -f /opt/user-operator/custom-config/log4j2.properties ];
then
    export JAVA_OPTS="${JAVA_OPTS} -Dlog4j2.configurationFile=file:/opt/user-operator/custom-config/log4j2.properties"
fi

if [ -n "$STRIMZI_JAVA_SYSTEM_PROPERTIES" ]; then
    export JAVA_OPTS="${JAVA_OPTS} ${STRIMZI_JAVA_SYSTEM_PROPERTIES}"
fi

if [ -n "$STRIMZI_JAVA_OPTS" ]; then
    export JAVA_OPTS="${JAVA_OPTS} ${STRIMZI_JAVA_OPTS}"
fi

export JAVA_CLASSPATH=$JAVA_CLASSPATH:lib/io.strimzi.@project.build.finalName@.@project.packaging@:@project.dist.classpath@
export JAVA_MAIN=io.strimzi.operator.user.Main
exec "${STRIMZI_HOME}/bin/launch_java.sh"
