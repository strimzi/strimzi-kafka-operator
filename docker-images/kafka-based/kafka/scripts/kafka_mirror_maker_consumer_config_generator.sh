#!/usr/bin/env bash
set -e

SECURITY_PROTOCOL=PLAINTEXT

if [ "$KAFKA_MIRRORMAKER_TLS_CONSUMER" = "true" ]; then
    SECURITY_PROTOCOL="SSL"

    if [ -n "$KAFKA_MIRRORMAKER_TRUSTED_CERTS_CONSUMER" ]; then
        TLS_CONFIGURATION=$(cat <<EOF
# TLS / SSL
ssl.truststore.location=/tmp/kafka/consumer.truststore.p12
ssl.truststore.password=${CERTS_STORE_PASSWORD}
ssl.truststore.type=PKCS12
EOF
)
    fi

    if [ -n "$KAFKA_MIRRORMAKER_TLS_AUTH_CERT_CONSUMER" ] && [ -n "$KAFKA_MIRRORMAKER_TLS_AUTH_KEY_CONSUMER" ]; then
        TLS_AUTH_CONFIGURATION=$(cat <<EOF
ssl.keystore.location=/tmp/kafka/consumer.keystore.p12
ssl.keystore.password=${CERTS_STORE_PASSWORD}
ssl.keystore.type=PKCS12
EOF
)
    fi
fi

if [ -n "$KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER" ]; then
    if [ "$SECURITY_PROTOCOL" = "SSL" ]; then
        SECURITY_PROTOCOL="SASL_SSL"
    else
        SECURITY_PROTOCOL="SASL_PLAINTEXT"
    fi

    if [ "$KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER" = "plain" ]; then
        PASSWORD=$(cat "/opt/kafka/consumer-password/$KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_CONSUMER")
        SASL_MECHANISM="PLAIN"
        JAAS_CONFIG="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${KAFKA_MIRRORMAKER_SASL_USERNAME_CONSUMER}\" password=\"${PASSWORD}\";"
    elif [ "$KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER" = "scram-sha-512" ]; then
        PASSWORD=$(cat "/opt/kafka/consumer-password/$KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_CONSUMER")
        SASL_MECHANISM="SCRAM-SHA-512"
        JAAS_CONFIG="org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${KAFKA_MIRRORMAKER_SASL_USERNAME_CONSUMER}\" password=\"${PASSWORD}\";"
    elif [ "$KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER" = "scram-sha-256" ]; then
        PASSWORD=$(cat "/opt/kafka/consumer-password/$KAFKA_MIRRORMAKER_SASL_PASSWORD_FILE_CONSUMER")
        SASL_MECHANISM="SCRAM-SHA-256"
        JAAS_CONFIG="org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${KAFKA_MIRRORMAKER_SASL_USERNAME_CONSUMER}\" password=\"${PASSWORD}\";"
    elif [ "$KAFKA_MIRRORMAKER_SASL_MECHANISM_CONSUMER" = "oauth" ]; then
        if [ -n "$KAFKA_MIRRORMAKER_OAUTH_ACCESS_TOKEN_CONSUMER" ]; then
            OAUTH_ACCESS_TOKEN="oauth.access.token=\"$KAFKA_MIRRORMAKER_OAUTH_ACCESS_TOKEN_CONSUMER\""
        fi

        if [ -n "$KAFKA_MIRRORMAKER_OAUTH_REFRESH_TOKEN_CONSUMER" ]; then
            OAUTH_REFRESH_TOKEN="oauth.refresh.token=\"$KAFKA_MIRRORMAKER_OAUTH_REFRESH_TOKEN_CONSUMER\""
        fi

        if [ -n "$KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_CONSUMER" ]; then
            OAUTH_CLIENT_SECRET="oauth.client.secret=\"$KAFKA_MIRRORMAKER_OAUTH_CLIENT_SECRET_CONSUMER\""
        fi

        if [ -n "$KAFKA_MIRRORMAKER_OAUTH_PASSWORD_GRANT_PASSWORD_CONSUMER" ]; then
            OAUTH_PASSWORD_GRANT_PASSWORD="oauth.password.grant.password=\"$KAFKA_MIRRORMAKER_OAUTH_PASSWORD_GRANT_PASSWORD_CONSUMER\""
        fi

        if [ -f "/tmp/kafka/consumer-oauth.keystore.p12" ]; then
            OAUTH_TRUSTSTORE="oauth.ssl.truststore.location=\"/tmp/kafka/consumer-oauth.keystore.p12\" oauth.ssl.truststore.password=\"${CERTS_STORE_PASSWORD}\" oauth.ssl.truststore.type=\"PKCS12\""
        fi

        SASL_MECHANISM="OAUTHBEARER"
        JAAS_CONFIG="org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ${KAFKA_MIRRORMAKER_OAUTH_CONFIG_CONSUMER} ${OAUTH_CLIENT_SECRET} ${OAUTH_REFRESH_TOKEN} ${OAUTH_ACCESS_TOKEN} ${OAUTH_PASSWORD_GRANT_PASSWORD} ${OAUTH_TRUSTSTORE};"
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