#!/usr/bin/env bash

# Parameters:
# $1: Path to the new truststore
# $2: Truststore password
# $3: Public key to be imported
# $4: Alias of the certificate
function create_truststore {
   keytool -keystore $1 -storepass $2 -noprompt -alias $4 -import -file $3 -storetype PKCS12
}

# Parameters:
# $1: Path to the new keystore
# $2: Truststore password
# $3: Public key to be imported
# $4: Private key to be imported
# $5: CA public key to be imported
# $6: Alias of the certificate
function create_keystore {
   RANDFILE=/tmp/.rnd openssl pkcs12 -export -in $3 -inkey $4 -chain -CAfile $5 -name $6 -password pass:$2 -out $1
}

# Searches the directory with the CAs and finds the CA matching our key.
# This is useful during certificate renewals
#
# Parameters:
# $1: The directory with the CA certificates
# $2: Public key to be imported
function find_ca {
    for ca in $1/*; do
        openssl verify -CAfile $ca $2 &> /dev/null

        if [ $? -eq 0 ]; then
            echo $ca
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
CA=$(find_ca /opt/kafka/cluster-ca-certs /opt/kafka/broker-certs/$HOSTNAME.crt)

if [ ! -f "$CA" ]; then
    echo "No CA found. This exiting."
fi
echo "Found the right CA: $CA"

echo "Preparing keystore for replication and clienttls listener"
create_keystore /tmp/kafka/cluster.keystore.p12 $CERTS_STORE_PASSWORD \
    /opt/kafka/broker-certs/$HOSTNAME.crt \
    /opt/kafka/broker-certs/$HOSTNAME.key \
    $CA \
    $HOSTNAME
echo "Preparing keystore for replication and clienttls listener is complete"

echo "Preparing truststore for clienttls listener"
# Add each certificate to the trust store
STORE=/tmp/kafka/clients.truststore.p12
for CRT in /opt/kafka/client-ca-certs/*.crt; do
  ALIAS=$(basename "$CRT" .crt)
  echo "Adding $CRT to truststore $STORE with alias $ALIAS"
  create_truststore "$STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
done
echo "Preparing truststore for clienttls listener is complete"

if [ -d /opt/kafka/oauth-certs/client ]; then
  echo "Preparing truststore for OAuth on plain listener"
  # Add each certificate to the trust store
  STORE=/tmp/kafka/oauth-client.truststore.p12
  declare -i INDEX=0
  for CRT in /opt/kafka/oauth-certs/client/**/*; do
    ALIAS="oauth-${INDEX}"
    echo "Adding $CRT to truststore $STORE with alias $ALIAS"
    create_truststore "$STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
    INDEX+=1
  done
  echo "Preparing truststore for OAuth on plain listener"
fi

if [ -d /opt/kafka/oauth-certs/clienttls ]; then
  echo "Preparing truststore for OAuth on cienttls listener"
  # Add each certificate to the trust store
  STORE=/tmp/kafka/oauth-clienttls.truststore.p12
  declare -i INDEX=0
  for CRT in /opt/kafka/oauth-certs/clienttls/**/*; do
    ALIAS="oauth-${INDEX}"
    echo "Adding $CRT to truststore $STORE with alias $ALIAS"
    create_truststore "$STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
    INDEX+=1
  done
  echo "Preparing truststore for OAuth on cienttls listener"
fi

if [ -d /opt/kafka/oauth-certs/external ]; then
  echo "Preparing truststore for OAuth on external listener"
  # Add each certificate to the trust store
  STORE=/tmp/kafka/oauth-external.truststore.p12
  declare -i INDEX=0
  for CRT in /opt/kafka/oauth-certs/external/**/*; do
    ALIAS="oauth-${INDEX}"
    echo "Adding $CRT to truststore $STORE with alias $ALIAS"
    create_truststore "$STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
    INDEX+=1
  done
  echo "Preparing truststore for OAuth on external listener"
fi