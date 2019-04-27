#!/usr/bin/env bash

SECURITY_PROTOCOL=PLAINTEXT

if [ "$KAFKA_MIRRORMAKER_TLS_PRODUCER" = "true" ]; then
    SECURITY_PROTOCOL="SSL"

    if [ -n "$KAFKA_MIRRORMAKER_TRUSTED_CERTS_PRODUCER" ]; then
        TLS_CONFIGURATION=$(cat <<EOF
# TLS / SSL
ssl.truststore.location=/tmp/kafka/producer.truststore.p12
ssl.truststore.password=${CERTS_STORE_PASSWORD}
ssl.truststore.type=PKCS12
EOF
)
    fi

    if [ -n "$KAFKA_MIRRORMAKER_TLS_AUTH_CERT_PRODUCER" ] && [ -n "$KAFKA_MIRRORMAKER_TLS_AUTH_KEY_PRODUCER" ]; then
        TLS_AUTH_CONFIGURATION=$(cat <<EOF
ssl.keystore.location=/tmp/kafka/producer.keystore.p12
ssl.keystore.password=${CERTS_STORE_PASSWORD}
ssl.keystore.type=PKCS12
EOF
)
    fi
fi

if [ -n "$KAFKA_MIRRORMAKER_SASL_USERNAME_PRODUCER" ] && [ -n "$KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_PRODUCER" ]; then
    if [ "$SECURITY_PROTOCOL" = "SSL" ]; then
        SECURITY_PROTOCOL="SASL_SSL"
    else
        SECURITY_PROTOCOL="SASL_PLAINTEXT"
    fi

    PASSWORD=$(cat /opt/kafka/producer-password/$KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_PRODUCER)

    if [ "x$KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER" = "xplain" ]; then
        SASL_MECHANISM="PLAIN"
        JAAS_SECURITY_MODULE="plain.PlainLoginModule"
    elif [ "x$KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER" = "xscram-sha-512" ]; then
        SASL_MECHANISM="SCRAM-SHA-512"
        JAAS_SECURITY_MODULE="scram.ScramLoginModule"
    fi

    SASL_AUTH_CONFIGURATION=$(cat <<EOF
sasl.mechanism=${SASL_MECHANISM}
sasl.jaas.config=org.apache.kafka.common.security.${JAAS_SECURITY_MODULE} required username="${KAFKA_MIRRORMAKER_SASL_USERNAME_PRODUCER}" password="${PASSWORD}";
EOF
)
fi

# Write the config file
cat <<EOF
# Bootstrap servers
bootstrap.servers=${KAFKA_MIRRORMAKER_BOOTSTRAP_SERVERS_PRODUCER}
# Provided configuration
${KAFKA_MIRRORMAKER_CONFIGURATION_PRODUCER}

security.protocol=${SECURITY_PROTOCOL}
${TLS_CONFIGURATION}
${TLS_AUTH_CONFIGURATION}
${SASL_AUTH_CONFIGURATION}
EOF