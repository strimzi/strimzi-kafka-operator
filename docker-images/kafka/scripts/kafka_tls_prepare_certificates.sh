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

echo "Preparing truststore for replication listener"
# Add each certificate to the trust store
STORE=/tmp/kafka/cluster.truststore.p12
for CRT in /opt/kafka/cluster-ca-certs/*.crt; do
  ALIAS=$(basename "$CRT" .crt)
  echo "Adding $CRT to truststore $STORE with alias $ALIAS"
  create_truststore "$STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
done
echo "Preparing truststore for replication listener is complete"

echo "Looking for the right CA"
CA=$(find_ca /opt/kafka/cluster-ca-certs "/opt/kafka/broker-certs/$HOSTNAME.crt")

if [ ! -f "$CA" ]; then
    echo "No CA found. Thus exiting."
    exit 1
fi
echo "Found the right CA: $CA"

echo "Preparing keystore for replication and clienttls listener"
create_keystore /tmp/kafka/cluster.keystore.p12 "$CERTS_STORE_PASSWORD" \
    "/opt/kafka/broker-certs/$HOSTNAME.crt" \
    "/opt/kafka/broker-certs/$HOSTNAME.key" \
    "$CA" \
    "$HOSTNAME"
echo "Preparing keystore for replication and clienttls listener is complete"

CUSTOM_CERT_DIR="/opt/kafka/certificates/custom-tls-9093-certs"
if [ -d "$CUSTOM_CERT_DIR" ]; then
    echo "Preparing custom keystore for tls listener"
    create_keystore_without_ca_file /tmp/kafka/custom-tls-9093.keystore.p12 "$CERTS_STORE_PASSWORD" "${CUSTOM_CERT_DIR}/tls.crt" "${CUSTOM_CERT_DIR}/tls.key" custom-key
    echo "Preparing custom keystore for tls listener is complete"
fi

CUSTOM_CERT_DIR="/opt/kafka/certificates/custom-external-9094-certs"
if [ -d "$CUSTOM_CERT_DIR" ]; then
    echo "Preparing custom keystore for external listener"
    create_keystore_without_ca_file /tmp/kafka/custom-external-9094.keystore.p12 "$CERTS_STORE_PASSWORD" "${CUSTOM_CERT_DIR}/tls.crt" "${CUSTOM_CERT_DIR}/tls.key" custom-key
    echo "Preparing custom keystore for external listener is complete"
fi

echo "Preparing truststore for clienttls listener"
# Add each certificate to the trust store
STORE=/tmp/kafka/clients.truststore.p12
for CRT in /opt/kafka/client-ca-certs/*.crt; do
  ALIAS=$(basename "$CRT" .crt)
  echo "Adding $CRT to truststore $STORE with alias $ALIAS"
  create_truststore "$STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
done
echo "Preparing truststore for clienttls listener is complete"

OAUTH_CERT_DIR="/opt/kafka/certificates/oauth-plain-9092-certs"
OAUTH_STORE="/tmp/kafka/oauth-plain-9092.truststore.p12"
if [ -d "$OAUTH_CERT_DIR" ]; then
  echo "Preparing truststore for OAuth on PLAIN listener"

  # Add each certificate to the trust store
  declare -i INDEX=0
  for CRT in "$OAUTH_CERT_DIR"/**/*; do
    ALIAS="oauth-${INDEX}"
    echo "Adding $CRT to truststore $OAUTH_STORE with alias $ALIAS"
    create_truststore "$OAUTH_STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
    INDEX+=1
  done
  echo "Preparing truststore for OAuth on PLAIN listener is complete"
fi

OAUTH_CERT_DIR="/opt/kafka/certificates/oauth-tls-9093-certs"
OAUTH_STORE="/tmp/kafka/oauth-tls-9093.truststore.p12"
if [ -d "$OAUTH_CERT_DIR" ]; then
  echo "Preparing truststore for OAuth on TLS listener"

  # Add each certificate to the trust store
  declare -i INDEX=0
  for CRT in "$OAUTH_CERT_DIR"/**/*; do
    ALIAS="oauth-${INDEX}"
    echo "Adding $CRT to truststore $OAUTH_STORE with alias $ALIAS"
    create_truststore "$OAUTH_STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
    INDEX+=1
  done
  echo "Preparing truststore for OAuth on TLS listener is complete"
fi

OAUTH_CERT_DIR="/opt/kafka/certificates/oauth-external-9094-certs"
OAUTH_STORE="/tmp/kafka/oauth-external-9094.truststore.p12"
if [ -d "$OAUTH_CERT_DIR" ]; then
  echo "Preparing truststore for OAuth on external listener"

  # Add each certificate to the trust store
  declare -i INDEX=0
  for CRT in "$OAUTH_CERT_DIR"/**/*; do
    ALIAS="oauth-${INDEX}"
    echo "Adding $CRT to truststore $OAUTH_STORE with alias $ALIAS"
    create_truststore "$OAUTH_STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
    INDEX+=1
  done
  echo "Preparing truststore for OAuth on external listener is complete"
fi

AUTHZ_KEYCLOAK_DIR="/opt/kafka/certificates/authz-keycloak-certs"
AUTHZ_KEYCLOAK_STORE="/tmp/kafka/authz-keycloak.truststore.p12"
if [ -d "$AUTHZ_KEYCLOAK_DIR" ]; then
  echo "Preparing truststore for Authorization with Keycloak"

  # Add each certificate to the trust store
  declare -i INDEX=0
  for CRT in "$AUTHZ_KEYCLOAK_DIR"/**/*; do
    ALIAS="authz-keycloak-${INDEX}"
    echo "Adding $CRT to truststore $AUTHZ_KEYCLOAK_STORE with alias $ALIAS"
    create_truststore "$AUTHZ_KEYCLOAK_STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
    INDEX+=1
  done
  echo "Preparing truststore for Authorization with Keycloak is complete"
fi