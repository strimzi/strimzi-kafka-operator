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

export JAVA_CLASSPATH=lib/io.strimzi.topic-operator-0.12.0-SNAPSHOT.jar:lib/com.squareup.okhttp3.okhttp-3.9.1.jar:lib/com.squareup.okio.okio-1.13.0.jar:lib/io.netty.netty-codec-dns-4.1.19.Final.jar:lib/io.netty.netty-codec-4.1.19.Final.jar:lib/io.strimzi.certificate-manager-0.12.0-SNAPSHOT.jar:lib/com.101tec.zkclient-0.11.jar:lib/io.netty.netty-codec-http2-4.1.19.Final.jar:lib/org.apache.logging.log4j.log4j-core-2.11.1.jar:lib/dk.brics.automaton.automaton-1.11-8.jar:lib/com.fasterxml.jackson.core.jackson-databind-2.9.8.jar:lib/io.netty.netty-handler-4.1.19.Final.jar:lib/io.netty.netty-resolver-dns-4.1.19.Final.jar:lib/com.github.mifmif.generex-1.0.1.jar:lib/io.netty.netty-resolver-4.1.19.Final.jar:lib/com.fasterxml.jackson.dataformat.jackson-dataformat-yaml-2.9.8.jar:lib/io.fabric8.kubernetes-client-4.1.1.jar:lib/io.netty.netty-handler-proxy-4.1.19.Final.jar:lib/io.fabric8.zjsonpatch-0.3.0.jar:lib/org.apache.yetus.audience-annotations-0.5.0.jar:lib/com.fasterxml.jackson.module.jackson-module-jaxb-annotations-2.7.5.jar:lib/io.vertx.vertx-core-3.5.4.jar:lib/io.netty.netty-transport-4.1.19.Final.jar:lib/io.netty.netty-codec-http-4.1.19.Final.jar:lib/io.netty.netty-buffer-4.1.19.Final.jar:lib/org.lz4.lz4-java-1.5.0.jar:lib/io.netty.netty-codec-socks-4.1.19.Final.jar:lib/io.strimzi.api-0.12.0-SNAPSHOT.jar:lib/org.apache.zookeeper.zookeeper-3.4.13.jar:lib/org.apache.kafka.kafka-clients-2.1.1.jar:lib/io.netty.netty-common-4.1.19.Final.jar:lib/org.slf4j.slf4j-api-1.7.25.jar:lib/com.fasterxml.jackson.core.jackson-annotations-2.9.8.jar:lib/io.fabric8.kubernetes-model-4.1.1.jar:lib/org.yaml.snakeyaml-1.23.jar:lib/org.slf4j.jul-to-slf4j-1.7.13.jar:lib/javax.validation.validation-api-1.1.0.Final.jar:lib/org.apache.logging.log4j.log4j-slf4j-impl-2.11.1.jar:lib/com.squareup.okhttp3.logging-interceptor-3.9.1.jar:lib/com.github.luben.zstd-jni-1.3.7-1.jar:lib/com.fasterxml.jackson.core.jackson-core-2.9.8.jar:lib/io.strimzi.operator-common-0.12.0-SNAPSHOT.jar:lib/io.fabric8.openshift-client-4.1.1.jar:lib/org.apache.logging.log4j.log4j-api-2.11.1.jar:lib/org.xerial.snappy.snappy-java-1.1.7.2.jar
export JAVA_MAIN=io.strimzi.operator.topic.Main
exec ${STRIMZI_HOME}/bin/launch_java.sh
