#!/usr/bin/env bash
set -e

# Load predefined functions for preparing trust- and keystores
source ./tls_utils.sh

echo "Preparing truststore for replication listener"
# Add each certificate to the trust store
STORE=/tmp/kafka/cluster.truststore.p12
rm -f "$STORE"
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
STORE=/tmp/kafka/cluster.keystore.p12
rm -f "$STORE"
create_keystore "$STORE" "$CERTS_STORE_PASSWORD" \
    "/opt/kafka/broker-certs/$HOSTNAME.crt" \
    "/opt/kafka/broker-certs/$HOSTNAME.key" \
    "$CA" \
    "$HOSTNAME"
echo "Preparing keystore for replication and clienttls listener is complete"

regex="^\/opt\/kafka\/certificates\/(custom|oauth)-(.+)-(.+)-certs$"
for CERT_DIR in /opt/kafka/certificates/*; do
  if [[ $CERT_DIR =~ $regex ]]; then
    listener=${BASH_REMATCH[1]}-${BASH_REMATCH[2]}-${BASH_REMATCH[3]}
    echo "Preparing store for $listener listener"
    if [[ ${BASH_REMATCH[1]} == "custom"  ]]; then
      echo "Creating keystore /tmp/kafka/$listener.keystore.p12"
      rm -f /tmp/kafka/"$listener".keystore.p12
      create_keystore_without_ca_file /tmp/kafka/"$listener".keystore.p12 "$CERTS_STORE_PASSWORD" "${CERT_DIR}/tls.crt" "${CERT_DIR}/tls.key" custom-key
    elif [[ ${BASH_REMATCH[1]} == "oauth"  ]]; then
      trusted_certs="STRIMZI_${BASH_REMATCH[2]^^}_${BASH_REMATCH[3]}_OAUTH_TRUSTED_CERTS"
      if [ -n "${!trusted_certs}" ]; then
        prepare_truststore "/tmp/kafka/$listener.truststore.p12" "$CERTS_STORE_PASSWORD" "$CERT_DIR" "${!trusted_certs}"
      fi
    fi
    echo "Preparing store for ${BASH_REMATCH[1]} ${BASH_REMATCH[2]} listener is complete"  
  fi
done

echo "Preparing truststore for client authentication"
# Add each certificate to the trust store
STORE=/tmp/kafka/clients.truststore.p12
rm -f "$STORE"
for CRT in /opt/kafka/client-ca-certs/*.crt; do
  ALIAS=$(basename "$CRT" .crt)
  echo "Adding $CRT to truststore $STORE with alias $ALIAS"
  create_truststore "$STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
done
echo "Preparing truststore for client authentication is complete"

if [ -n "$STRIMZI_OPA_AUTHZ_TRUSTED_CERTS" ]; then
  echo "Preparing Open Policy Agent authorization truststore"
  prepare_truststore "/tmp/kafka/authz-opa.truststore.p12" "$CERTS_STORE_PASSWORD" "/opt/kafka/certificates/authz-opa-certs" "$STRIMZI_OPA_AUTHZ_TRUSTED_CERTS"
fi

if [ -n "$STRIMZI_KEYCLOAK_AUTHZ_TRUSTED_CERTS" ]; then
  echo "Preparing Keycloak authorization truststore"
  prepare_truststore "/tmp/kafka/authz-keycloak.truststore.p12" "$CERTS_STORE_PASSWORD" "/opt/kafka/certificates/authz-keycloak-certs" "$STRIMZI_KEYCLOAK_AUTHZ_TRUSTED_CERTS"
fi
