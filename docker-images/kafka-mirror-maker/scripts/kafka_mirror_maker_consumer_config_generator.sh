#!/bin/bash

SECURITY_PROTOCOL=PLAINTEXT

if [ -n "$KAFKA_MIRRORMAKER_TRUSTED_CERTS_CONSUMER" ]; then
    SECURITY_PROTOCOL="SSL"
    TLS_CONFIGURATION=$(cat <<EOF
# TLS / SSL
ssl.truststore.location=/tmp/kafka/consumer.truststore.p12
ssl.truststore.password=${CERTS_STORE_PASSWORD}
ssl.truststore.type=PKCS12
EOF
)

    if [ -n "$KAFKA_MIRRORMAKER_TLS_AUTH_CERT_CONSUMER" ] && [ -n "$KAFKA_MIRRORMAKER_TLS_AUTH_KEY_CONSUMER" ]; then
        TLS_AUTH_CONFIGURATION=$(cat <<EOF
ssl.keystore.location=/tmp/kafka/consumer.keystore.p12
ssl.keystore.password=${CERTS_STORE_PASSWORD}
ssl.keystore.type=PKCS12
EOF
)
    fi
fi

if [ -n "$KAFKA_MIRRORMAKER_SASL_USERNAME_CONSUMER" ] && [ -n "$KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_CONSUMER" ]; then
    if [ "$SECURITY_PROTOCOL" = "SSL" ]; then
        SECURITY_PROTOCOL="SASL_SSL"
    else
        SECURITY_PROTOCOL="SASL_PLAINTEXT"
    fi

    PASSWORD=$(cat /opt/kafka/consumer-password/$KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_CONSUMER)
    SASL_MECHANISM="SCRAM-SHA-512"

    SASL_AUTH_CONFIGURATION=$(cat <<EOF
sasl.mechanism=${SASL_MECHANISM}
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${KAFKA_MIRRORMAKER_SASL_USERNAME_CONSUMER}" password="${PASSWORD}";
EOF
)
fi

# Write the config file
cat <<EOF
# Bootstrap servers
bootstrap.servers=${KAFKA_MIRRORMAKER_BOOTSTRAP_SERVERS_CONSUMER}
# Consumer group
group.id=${KAFKA_MIRRORMAKER_GROUPID_CONSUMER}
# Provided configuration
${KAFKA_MIRRORMAKER_CONFIGURATION_CONSUMER}

security.protocol=${SECURITY_PROTOCOL}
${TLS_CONFIGURATION}
${TLS_AUTH_CONFIGURATION}
${SASL_AUTH_CONFIGURATION}
EOF