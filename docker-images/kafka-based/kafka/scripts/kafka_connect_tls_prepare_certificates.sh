#!/usr/bin/env bash
set -e

# Load predefined functions for preparing trust- and keystores
source ./tls_utils.sh

echo "Preparing truststore"
STORE=/tmp/kafka/cluster.truststore.p12
rm -f "$STORE"
IFS=';' read -ra CERTS <<< "${KAFKA_CONNECT_TRUSTED_CERTS}"
for cert in "${CERTS[@]}"
do
    create_truststore "$STORE" "$CERTS_STORE_PASSWORD" "/opt/kafka/connect-certs/$cert" "$cert"
done
echo "Preparing truststore is complete"

if [ -n "$KAFKA_CONNECT_TLS_AUTH_CERT" ] && [ -n "$KAFKA_CONNECT_TLS_AUTH_KEY" ]; then
    echo "Preparing keystore"
    STORE=/tmp/kafka/cluster.keystore.p12
    rm -f "$STORE"
    create_keystore_without_ca_file "$STORE" "$CERTS_STORE_PASSWORD" "/opt/kafka/connect-certs/${KAFKA_CONNECT_TLS_AUTH_CERT}" "/opt/kafka/connect-certs/${KAFKA_CONNECT_TLS_AUTH_KEY}" "${KAFKA_CONNECT_TLS_AUTH_CERT}"
    echo "Preparing keystore is complete"
fi

if [ -d /opt/kafka/oauth-certs ]; then
  echo "Preparing truststore for OAuth"
  # Add each certificate to the trust store
  STORE=/tmp/kafka/oauth.truststore.p12
  rm -f "$STORE"
  declare -i INDEX=0
  for CRT in /opt/kafka/oauth-certs/**/*; do
    ALIAS="oauth-${INDEX}"
    echo "Adding $CRT to truststore $STORE with alias $ALIAS"
    create_truststore "$STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
    INDEX+=1
  done
  echo "Preparing truststore for OAuth is complete"
fi