#!/usr/bin/env bash

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

    IFS=$'\n' read -rd '' -a CLUSTERS <<< "$KAFKA_MIRRORMAKER_2_SASL_PASSWORD_FILES_CLUSTERS"
    for cluster in "${CLUSTERS[@]}"
    do
        IFS='=' read -ra PASSWORD_FILE_CLUSTER <<< "${cluster}"
        export clusterAlias="${PASSWORD_FILE_CLUSTER[0]}"
        export passwordFile="${PASSWORD_FILE_CLUSTER[1]}"

        PASSWORD=$(cat /opt/kafka/connect-password/$passwordFile)
        SASL_AUTH_CONFIGURATION=$(cat <<EOF
${SASL_AUTH_CONFIGURATION}
${clusterAlias}.sasl.password=${PASSWORD}
EOF
)
    done
fi


# Write the config file
cat <<EOF
${TLS_CONFIGURATION}
${TLS_AUTH_CONFIGURATION}
${SASL_AUTH_CONFIGURATION}
EOF