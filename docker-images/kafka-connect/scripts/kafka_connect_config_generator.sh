#!/usr/bin/env bash

SECURITY_PROTOCOL=PLAINTEXT

if [ "$KAFKA_CONNECT_TLS" = "true" ]; then
    SECURITY_PROTOCOL="SSL"

    if [ -n "$KAFKA_CONNECT_TRUSTED_CERTS" ]; then
        TLS_CONFIGURATION=$(cat <<EOF
# TLS / SSL
ssl.truststore.location=/tmp/kafka/cluster.truststore.p12
ssl.truststore.password=${CERTS_STORE_PASSWORD}
ssl.truststore.type=PKCS12

producer.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12
producer.ssl.truststore.password=${CERTS_STORE_PASSWORD}

consumer.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12
consumer.ssl.truststore.password=${CERTS_STORE_PASSWORD}
EOF
)
    fi

    if [ -n "$KAFKA_CONNECT_TLS_AUTH_CERT" ] && [ -n "$KAFKA_CONNECT_TLS_AUTH_KEY" ]; then
        TLS_AUTH_CONFIGURATION=$(cat <<EOF
ssl.keystore.location=/tmp/kafka/cluster.keystore.p12
ssl.keystore.password=${CERTS_STORE_PASSWORD}
ssl.keystore.type=PKCS12

producer.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12
producer.ssl.keystore.password=${CERTS_STORE_PASSWORD}
producer.ssl.keystore.type=PKCS12

consumer.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12
consumer.ssl.keystore.password=${CERTS_STORE_PASSWORD}
consumer.ssl.keystore.type=PKCS12
EOF
)
    fi
fi

if [ -n "$KAFKA_CONNECT_SASL_USERNAME" ] && [ -n "$KAFKA_CONNECT_SASL_PASSWORD_FILE" ]; then
    if [ "$SECURITY_PROTOCOL" = "SSL" ]; then
        SECURITY_PROTOCOL="SASL_SSL"
    else
        SECURITY_PROTOCOL="SASL_PLAINTEXT"
    fi
    
    PASSWORD=$(cat /opt/kafka/connect-password/$KAFKA_CONNECT_SASL_PASSWORD_FILE)

    if [ "x$KAFKA_CONNECT_SASL_MECHANISM" = "xplain" ]; then
        SASL_MECHANISM="PLAIN"
        JAAS_SECURITY_MODULE="plain.PlainLoginModule"
    elif [ "x$KAFKA_CONNECT_SASL_MECHANISM" = "xscram-sha-512" ]; then
        SASL_MECHANISM="SCRAM-SHA-512"
        JAAS_SECURITY_MODULE="scram.ScramLoginModule"
    fi


    SASL_AUTH_CONFIGURATION=$(cat <<EOF
sasl.mechanism=${SASL_MECHANISM}
sasl.jaas.config=org.apache.kafka.common.security.${JAAS_SECURITY_MODULE} required username="${KAFKA_CONNECT_SASL_USERNAME}" password="${PASSWORD}";

producer.sasl.mechanism=${SASL_MECHANISM}
producer.sasl.jaas.config=org.apache.kafka.common.security.${JAAS_SECURITY_MODULE} required username="${KAFKA_CONNECT_SASL_USERNAME}" password="${PASSWORD}";

consumer.sasl.mechanism=${SASL_MECHANISM}
consumer.sasl.jaas.config=org.apache.kafka.common.security.${JAAS_SECURITY_MODULE} required username="${KAFKA_CONNECT_SASL_USERNAME}" password="${PASSWORD}";

EOF
)
fi

# Write the config file
cat <<EOF
# Bootstrap servers
bootstrap.servers=${KAFKA_CONNECT_BOOTSTRAP_SERVERS}
# REST Listeners
rest.port=8083
rest.advertised.host.name=$(hostname -I)
rest.advertised.port=8083
# Plugins
plugin.path=${KAFKA_CONNECT_PLUGIN_PATH}
# Provided configuration
${KAFKA_CONNECT_CONFIGURATION}

security.protocol=${SECURITY_PROTOCOL}
producer.security.protocol=${SECURITY_PROTOCOL}
consumer.security.protocol=${SECURITY_PROTOCOL}
${TLS_CONFIGURATION}
${TLS_AUTH_CONFIGURATION}
${SASL_AUTH_CONFIGURATION}
EOF