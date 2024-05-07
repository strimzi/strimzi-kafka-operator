#!/usr/bin/env bash
set -e

# Clean-up /tmp directory from files which might have remained from previous container restart
# We ignore any errors which might be caused by files injected by different agents which we do not have the rights to delete
rm -rfv /tmp/* || true

export JAVA_CLASSPATH=$JAVA_CLASSPATH:lib/io.strimzi.@project.build.finalName@.@project.packaging@:@project.dist.classpath@
export JAVA_MAIN=io.strimzi.operator.cluster.Main

if [ -z "$KUBERNETES_SERVICE_DNS_DOMAIN" ]; then
  KUBERNETES_SERVICE_DNS_DOMAIN=$(getent hosts kubernetes.default | head -1 | sed "s/.*\skubernetes.default.svc//" | sed "s/\.//")
  if [ -n "$KUBERNETES_SERVICE_DNS_DOMAIN" ]; then
    echo "Auto-detected KUBERNETES_SERVICE_DNS_DOMAIN: $KUBERNETES_SERVICE_DNS_DOMAIN"
    export KUBERNETES_SERVICE_DNS_DOMAIN
  else
    echo "Auto-detection of KUBERNETES_SERVICE_DNS_DOMAIN failed. The default value cluster.local will be used."
  fi
else
  echo "KUBERNETES_SERVICE_DNS_DOMAIN is already set. Skipping auto-detection."
fi

if [ -f /opt/strimzi/custom-config/log4j2.properties ]; then
    # if ConfigMap was not mounted and thus this file was not created, use properties file from the classpath
    export JAVA_OPTS="${JAVA_OPTS} -Dlog4j2.configurationFile=file:/opt/strimzi/custom-config/log4j2.properties"
else
    echo "Configuration file log4j2.properties not found. Using default static logging setting. Dynamic updates of logging configuration will not work."
fi

# Used to identify cluster operator instance when publishing events
if [[ -z "$STRIMZI_OPERATOR_NAME" ]]; then
  STRIMZI_OPERATOR_NAME="$(cat /proc/sys/kernel/hostname)"
  export STRIMZI_OPERATOR_NAME
fi

export JAVA_OPTS="${JAVA_OPTS} -Dvertx.cacheDirBase=/tmp/vertx-cache"

exec "${STRIMZI_HOME}/bin/launch_java.sh"
