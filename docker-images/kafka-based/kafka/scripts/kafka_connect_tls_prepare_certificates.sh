#!/usr/bin/env bash
set -e

# Load predefined functions for preparing trust- and keystores
source ./tls_utils.sh

if [ -n "$KAFKA_CONNECT_TRUSTED_CERTS" ]; then
    echo "Preparing Connect truststore"
    prepare_truststore "/tmp/kafka/cluster.truststore.p12" "$CERTS_STORE_PASSWORD" "/opt/kafka/connect-certs" "$KAFKA_CONNECT_TRUSTED_CERTS"
fi

if [ -n "$KAFKA_CONNECT_TLS_AUTH_CERT" ] && [ -n "$KAFKA_CONNECT_TLS_AUTH_KEY" ]; then
    echo "Preparing keystore"
    STORE=/tmp/kafka/cluster.keystore.p12
    rm -f "$STORE"
    create_keystore_without_ca_file "$STORE" "$CERTS_STORE_PASSWORD" "/opt/kafka/connect-certs/${KAFKA_CONNECT_TLS_AUTH_CERT}" "/opt/kafka/connect-certs/${KAFKA_CONNECT_TLS_AUTH_KEY}" "${KAFKA_CONNECT_TLS_AUTH_CERT}"
    echo "Preparing keystore is complete"
fi

if [ -n "$KAFKA_CONNECT_OAUTH_TRUSTED_CERTS" ]; then
    echo "Preparing OAuth truststore"
    prepare_truststore "/tmp/kafka/oauth.truststore.p12" "$CERTS_STORE_PASSWORD" "/opt/kafka/oauth-certs" "$KAFKA_CONNECT_OAUTH_TRUSTED_CERTS"
fi
