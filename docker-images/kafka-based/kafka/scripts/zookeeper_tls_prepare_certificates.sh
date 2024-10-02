#!/usr/bin/env bash
set -e

# Load predefined functions for preparing trust- and keystores
source ./tls_utils.sh

echo "Preparing truststore"
# Add each certificate to the trust store
STORE=/tmp/zookeeper/cluster.truststore.p12
rm -f "$STORE"
for CRT in /opt/kafka/cluster-ca-certs/*.crt; do
  ALIAS=$(basename "$CRT" .crt)
  echo "Adding $CRT to truststore $STORE with alias $ALIAS"
  create_truststore "$STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
done
echo "Preparing truststore is complete"

echo "Looking for the CA matching the server certificate"
CA=$(find_ca "/opt/kafka/cluster-ca-certs" "/opt/kafka/zookeeper-node-certs/$HOSTNAME.crt")

if [ ! -f "$CA" ]; then
    echo "No CA matching the server certificate found. This process will exit with failure."
    exit 1
fi

echo "CA matching the server certificate found: $CA"

echo "Preparing keystore for client and quorum listeners"
STORE=/tmp/zookeeper/cluster.keystore.p12
rm -f "$STORE"
create_keystore "$STORE" "$CERTS_STORE_PASSWORD" \
    "/opt/kafka/zookeeper-node-certs/$HOSTNAME.crt" \
    "/opt/kafka/zookeeper-node-certs/$HOSTNAME.key" \
    "$CA" \
    "$HOSTNAME"
echo "Preparing keystore for client and quorum listeners is complete"
