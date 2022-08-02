#!/usr/bin/env bash
set -e

if [ "$KAFKA_MIRRORMAKER_2_TLS_CLUSTERS" = "true" ]; then

    if [ -n "$KAFKA_MIRRORMAKER_2_TRUSTED_CERTS_CLUSTERS" ]; then
        TLS_CONFIGURATION=$(cat <<EOF
# TLS / SSL
ssl.truststore.password=${CERTS_STORE_PASSWORD}
EOF
)
    fi

    if [ -n "$KAFKA_MIRRORMAKER_2_TLS_AUTH_CERTS_CLUSTERS" ] && [ -n "$KAFKA_MIRRORMAKER_2_TLS_AUTH_KEYS_CLUSTERS" ]; then
        TLS_AUTH_CONFIGURATION=$(cat <<EOF
ssl.keystore.password=${CERTS_STORE_PASSWORD}
EOF
)
    fi
fi

if [ -n "$KAFKA_MIRRORMAKER_2_SASL_PASSWORD_FILES_CLUSTERS" ]; then

    SASL_AUTH_CONFIGURATION="# SASL"

    IFS=$'\n' read -rd '' -a CLUSTERS <<< "$KAFKA_MIRRORMAKER_2_SASL_PASSWORD_FILES_CLUSTERS" || true
    for cluster in "${CLUSTERS[@]}"
    do
        IFS='=' read -ra PASSWORD_FILE_CLUSTER <<< "${cluster}"
        export clusterAlias="${PASSWORD_FILE_CLUSTER[0]}"
        export passwordFile="${PASSWORD_FILE_CLUSTER[1]}"

        PASSWORD=$(cat "/opt/kafka/mm2-password/$clusterAlias/$passwordFile")
        SASL_AUTH_CONFIGURATION=$(cat <<EOF
${SASL_AUTH_CONFIGURATION}
${clusterAlias}.sasl.password=${PASSWORD}
EOF
)
    done
fi

if [ -n "$KAFKA_MIRRORMAKER_2_OAUTH_TRUSTED_CERTS" ]; then
    OAUTH_TRUSTED_CERTS_CONFIGURATION=$(cat <<EOF
# OAuth trusted certs
oauth.ssl.truststore.password=${CERTS_STORE_PASSWORD}
EOF
)
fi

if [ -n "$KAFKA_MIRRORMAKER_2_OAUTH_CLIENT_SECRETS_CLUSTERS" ]; then

    OAUTH_CLIENT_SECRETS_CONFIGURATION="# OAuth client secrets"

    IFS=$'\n' read -rd '' -a CLUSTERS <<< "$KAFKA_MIRRORMAKER_2_OAUTH_CLIENT_SECRETS_CLUSTERS" || true
    for cluster in "${CLUSTERS[@]}"
    do
        IFS='=' read -ra CLIENT_SECRET_CLUSTER <<< "${cluster}"
        export clusterAlias="${CLIENT_SECRET_CLUSTER[0]}"
        export clientSecretFile="${CLIENT_SECRET_CLUSTER[1]}"

        OAUTH_CLIENT_SECRET=$(cat "/opt/kafka/mm2-oauth/$clusterAlias/$clientSecretFile")
        OAUTH_CLIENT_SECRETS_CONFIGURATION=$(cat <<EOF
${OAUTH_CLIENT_SECRETS_CONFIGURATION}
${clusterAlias}.oauth.client.secret=${OAUTH_CLIENT_SECRET}
EOF
)
    done
fi

if [ -n "$KAFKA_MIRRORMAKER_2_OAUTH_ACCESS_TOKENS_CLUSTERS" ]; then

    OAUTH_ACCESS_TOKENS_CONFIGURATION="# OAuth access tokens"

    IFS=$'\n' read -rd '' -a CLUSTERS <<< "$KAFKA_MIRRORMAKER_2_OAUTH_ACCESS_TOKENS_CLUSTERS" || true
    for cluster in "${CLUSTERS[@]}"
    do
        IFS='=' read -ra ACCESS_TOKEN_CLUSTER <<< "${cluster}"
        export clusterAlias="${ACCESS_TOKEN_CLUSTER[0]}"
        export accessTokenFile="${ACCESS_TOKEN_CLUSTER[1]}"

        OAUTH_ACCESS_TOKEN=$(cat "/opt/kafka/mm2-oauth/$clusterAlias/$accessTokenFile")
        OAUTH_ACCESS_TOKENS_CONFIGURATION=$(cat <<EOF
${OAUTH_ACCESS_TOKENS_CONFIGURATION}
${clusterAlias}.oauth.access.token=${OAUTH_ACCESS_TOKEN}
EOF
)
    done
fi

if [ -n "$KAFKA_MIRRORMAKER_2_OAUTH_REFRESH_TOKENS_CLUSTERS" ]; then

    OAUTH_REFRESH_TOKENS_CONFIGURATION="# OAuth refresh tokens"

    IFS=$'\n' read -rd '' -a CLUSTERS <<< "$KAFKA_MIRRORMAKER_2_OAUTH_REFRESH_TOKENS_CLUSTERS" || true
    for cluster in "${CLUSTERS[@]}"
    do
        IFS='=' read -ra REFRESH_TOKEN_CLUSTER <<< "${cluster}"
        export clusterAlias="${REFRESH_TOKEN_CLUSTER[0]}"
        export refreshTokenFile="${REFRESH_TOKEN_CLUSTER[1]}"

        OAUTH_REFRESH_TOKEN=$(cat "/opt/kafka/mm2-oauth/$clusterAlias/$refreshTokenFile")
        OAUTH_REFRESH_TOKENS_CONFIGURATION=$(cat <<EOF
${OAUTH_REFRESH_TOKENS_CONFIGURATION}
${clusterAlias}.oauth.refresh.token=${OAUTH_REFRESH_TOKEN}
EOF
)
    done
fi

if [ -n "$KAFKA_MIRRORMAKER_2_OAUTH_PASSWORDS_CLUSTERS" ]; then

    OAUTH_PASSWORDS_CONFIGURATION="# OAuth passwords"

    IFS=$'\n' read -rd '' -a CLUSTERS <<< "$KAFKA_MIRRORMAKER_2_OAUTH_PASSWORDS_CLUSTERS" || true
    for cluster in "${CLUSTERS[@]}"
    do
        IFS='=' read -ra PASSWORD_CLUSTER <<< "${cluster}"
        export clusterAlias="${PASSWORD_CLUSTER[0]}"
        export passwordFile="${PASSWORD_CLUSTER[1]}"

        OAUTH_PASSWORD_GRANT_PASSWORD=$(cat "/opt/kafka/mm2-oauth/$clusterAlias/$passwordFile")
        OAUTH_PASSWORDS_CONFIGURATION=$(cat <<EOF
${OAUTH_PASSWORDS_CONFIGURATION}
${clusterAlias}.oauth.password.grant.password=${OAUTH_PASSWORD_GRANT_PASSWORD}
EOF
)
    done
fi

# Write the config file
cat <<EOF
${TLS_CONFIGURATION}
${TLS_AUTH_CONFIGURATION}
${SASL_AUTH_CONFIGURATION}
${OAUTH_TRUSTED_CERTS_CONFIGURATION}
${OAUTH_CLIENT_SECRETS_CONFIGURATION}
${OAUTH_ACCESS_TOKENS_CONFIGURATION}
${OAUTH_REFRESH_TOKENS_CONFIGURATION}
${OAUTH_PASSWORDS_CONFIGURATION}
EOF