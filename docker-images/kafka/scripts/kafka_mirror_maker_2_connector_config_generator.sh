#!/usr/bin/env bash

if [ "$KAFKA_MIRRORMAKER_2_TLS_CLUSTERS" = "true" ]; then
    TLS_CONFIGURATION=$(cat <<EOF
# TLS / SSL
ssl.truststore.password=${CERTS_STORE_PASSWORD}
EOF
)
fi

# Write the config file
cat <<EOF
${TLS_CONFIGURATION}
EOF