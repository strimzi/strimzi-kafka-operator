#!/usr/bin/env bash
set -e

# Parameters:
# $1: Path to the new truststore
# $2: Truststore password
# $3: Public key to be imported
# $4: Alias of the certificate
function create_truststore {
   keytool -keystore "$1" -storepass "$2" -noprompt -alias "$4" -import -file "$3" -storetype PKCS12
}

# Parameters:
# $1: Path to the new keystore
# $2: Truststore password
# $3: Public key to be imported
# $4: Private key to be imported
# $5: CA public key to be imported
# $6: Alias of the certificate
function create_keystore {
   RANDFILE=/tmp/.rnd openssl pkcs12 -export -in "$3" -inkey "$4" -chain -CAfile "$5" -name "$6" -password pass:"$2" -out "$1"
}

# Parameters:
# $1: Path to the new keystore
# $2: Truststore password
# $3: Public key to be imported
# $4: Private key to be imported
# $5: Alias of the certificate
function create_keystore_without_ca_file {
   RANDFILE=/tmp/.rnd openssl pkcs12 -export -in "$3" -inkey "$4" -name "$5" -password pass:"$2" -out "$1"
}

# Searches the directory with the CAs and finds the CA matching our key.
# This is useful during certificate renewals
#
# Parameters:
# $1: The directory with the CA certificates
# $2: Public key to be imported
function find_ca {
    for ca in "$1"/*; do
        if openssl verify -CAfile "$ca" "$2" &> /dev/null; then
            echo "$ca"
        fi
    done
}

echo "Preparing truststore"
# Add each certificate to the trust store
STORE=/tmp/zookeeper/cluster.truststore.p12
for CRT in /opt/kafka/cluster-ca-certs/*.crt; do
  ALIAS=$(basename "$CRT" .crt)
  echo "Adding $CRT to truststore $STORE with alias $ALIAS"
  create_truststore "$STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
done
echo "Preparing truststore is complete"

echo "Looking for the right CA"
CA=$(find_ca "/opt/kafka/cluster-ca-certs" "/opt/kafka/zookeeper-node-certs/$HOSTNAME.crt")

if [ ! -f "$CA" ]; then
    echo "No CA found. Thus exiting."
    exit 1
fi
echo "Found the right CA: $CA"

echo "Preparing keystore for client and quorum listeners"
create_keystore /tmp/zookeeper/cluster.keystore.p12 "$CERTS_STORE_PASSWORD" \
    "/opt/kafka/zookeeper-node-certs/$HOSTNAME.crt" \
    "/opt/kafka/zookeeper-node-certs/$HOSTNAME.key" \
    "$CA" \
    "$HOSTNAME"
echo "Preparing keystore for client and quorum listeners is complete"
