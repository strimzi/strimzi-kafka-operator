#!/usr/bin/env bash
set -e
set +x

# Load predefined functions for preparing trust- and keystores
# Shellcheck SC1091 is disabled because the path does not work locally, but does inside the container
# shellcheck disable=SC1091
source ../kafka/tls_utils.sh

echo "Preparing truststore for Cruise Control"
STORE=/tmp/cruise-control/replication.truststore.p12
rm -f "$STORE"
for CRT in /etc/cruise-control/cluster-ca-certs/*.crt; do
  ALIAS=$(basename "$CRT" .crt)
  echo "Adding $CRT to truststore $STORE with alias $ALIAS"
  create_truststore "$STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
done
echo "Preparing truststore for Cruise Control is complete"

echo "Preparing keystore for Cruise Control"
STORE=/tmp/cruise-control/cruise-control.keystore.p12
rm -f "$STORE"
create_keystore_without_ca_file "$STORE" "$CERTS_STORE_PASSWORD" \
    /etc/cruise-control/cc-certs/cruise-control.crt \
    /etc/cruise-control/cc-certs/cruise-control.key \
    cruise-control
echo "Preparing keystore for Cruise Control is complete"