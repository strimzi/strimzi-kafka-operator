#!/usr/bin/env bash
set -e

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

if [ -n "$KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER" ]; then
    if [ "$SECURITY_PROTOCOL" = "SSL" ]; then
        SECURITY_PROTOCOL="SASL_SSL"
    else
        SECURITY_PROTOCOL="SASL_PLAINTEXT"
    fi

    if [ "$KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER" = "plain" ]; then
        PASSWORD=$(cat "/opt/kafka/producer-password/$KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_PRODUCER")
        SASL_MECHANISM="PLAIN"
        JAAS_CONFIG="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${KAFKA_MIRRORMAKER_SASL_USERNAME_PRODUCER}\" password=\"${PASSWORD}\";"
    elif [ "$KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER" = "scram-sha-512" ]; then
        PASSWORD=$(cat "/opt/kafka/producer-password/$KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_PRODUCER")
        SASL_MECHANISM="SCRAM-SHA-512"
        JAAS_CONFIG="org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${KAFKA_MIRRORMAKER_SASL_USERNAME_PRODUCER}\" password=\"${PASSWORD}\";"
    elif [ "$KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER" = "scram-sha-256" ]; then
        PASSWORD=$(cat "/opt/kafka/producer-password/$KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_PRODUCER")
        SASL_MECHANISM="SCRAM-SHA-256"
        JAAS_CONFIG="org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${KAFKA_MIRRORMAKER_SASL_USERNAME_PRODUCER}\" password=\"${PASSWORD}\";"
    elif [ "$KAFKA_MIRRORMAKER_SASL_MECHANISM_PRODUCER" = "oauth" ]; then
        if [ -n "$KAFKA_MIRRORMAKER_OAUTH_ACCESS_TOKEN_PRODUCER" ]; then
            OAUTH_ACCESS_TOKEN="oauth.access.token=\"$KAFKA_MIRRORMAKER_OAUTH_ACCESS_TOKEN_PRODUCER\""
        fi

        if [ -n "$KAFKA_MIRRORMAKER_OAUTH_REFRESH_TOKEN_PRODUCER" ]; then
            OAUTH_REFRESH_TOKEN="oauth.refresh.token=\"$KAFKA_MIRRORMAKER_OAUTH_REFRESH_TOKEN_PRODUCER\""
        fi

        if [ -n "$KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_PRODUCER" ]; then
            OAUTH_CLIENT_SECRET="oauth.client.secret=\"$KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_PRODUCER\""
        fi

        if [ -n "$KAFKA_MIRRORMAKER_OAUTH_PASSWORD_GRANT_PASSWORD_CONSUMER" ]; then
            OAUTH_PASSWORD_GRANT_PASSWORD="oauth.password.grant.password=\"$KAFKA_MIRRORMAKER_OAUTH_PASSWORD_GRANT_PASSWORD_CONSUMER\""
        fi

        if [ -f "/tmp/kafka/producer-oauth.keystore.p12" ]; then
            OAUTH_TRUSTSTORE="oauth.ssl.truststore.location=\"/tmp/kafka/producer-oauth.keystore.p12\" oauth.ssl.truststore.password=\"${CERTS_STORE_PASSWORD}\" oauth.ssl.truststore.type=\"PKCS12\""
        fi

        SASL_MECHANISM="OAUTHBEARER"
        JAAS_CONFIG="org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ${KAFKA_MIRRORMAKER_OAUTH_CONFIG_PRODUCER} ${OAUTH_CLIENT_SECRET} ${OAUTH_REFRESH_TOKEN} ${OAUTH_ACCESS_TOKEN} ${OAUTH_PASSWORD_GRANT_PASSWORD} ${OAUTH_TRUSTSTORE};"
        OAUTH_CALLBACK_CLASS="sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler"
    fi

    SASL_AUTH_CONFIGURATION=$(cat <<EOF
sasl.mechanism=${SASL_MECHANISM}
sasl.jaas.config=${JAAS_CONFIG}
${OAUTH_CALLBACK_CLASS}
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