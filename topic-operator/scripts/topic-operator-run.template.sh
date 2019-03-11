#!/usr/bin/env bash
if [ -f /opt/topic-operator/custom-config/log4j2.properties ];
then
    export JAVA_OPTS="${JAVA_OPTS} -Dlog4j2.configurationFile=file:/opt/topic-operator/custom-config/log4j2.properties"
fi

if [ "$STRIMZI_TLS_ENABLED" = "true" ]; then
    if [ -z "$STRIMZI_TRUSTSTORE_LOCATION" ] && [ -z "$STRIMZI_KEYSTORE_LOCATION" ]; then
        # Generate temporary keystore password
        export CERTS_STORE_PASSWORD=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)

        mkdir -p /tmp/topic-operator

        # Import certificates into keystore and truststore
        ${STRIMZI_HOME}/bin/tls_prepare_certificates.sh

        export STRIMZI_TRUSTSTORE_LOCATION=/tmp/topic-operator/replication.truststore.p12
        export STRIMZI_TRUSTSTORE_PASSWORD=$CERTS_STORE_PASSWORD

        export STRIMZI_KEYSTORE_LOCATION=/tmp/topic-operator/replication.keystore.p12
        export STRIMZI_KEYSTORE_PASSWORD=$CERTS_STORE_PASSWORD
    fi
fi

export JAVA_CLASSPATH=${TP_CLASSPATH}
export JAVA_MAIN=${TP_MAIN}
exec ${STRIMZI_HOME}/bin/launch_java.sh
