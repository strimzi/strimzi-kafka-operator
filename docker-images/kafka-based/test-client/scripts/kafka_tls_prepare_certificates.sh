#!/usr/bin/env bash
set -e

# Load predefined functions for preparing trust- and keystores
source ./tls_utils.sh

USER=$1
TRUSTSTORE_LOCATION=$(eval "echo \$$(echo TRUSTSTORE_LOCATION_${USER})")
CA_LOCATION=$(eval "echo \$$(echo CA_LOCATION_${USER})")
KEYSTORE_LOCATION=$(eval "echo \$$(echo KEYSTORE_LOCATION_${USER})")
USER_LOCATION=$(eval "echo \$$(echo USER_LOCATION_${USER})")
KAFKA_USER=$(eval "echo \$$(echo KAFKA_USER_${USER})")

if [ -n "${TRUSTSTORE_LOCATION}" ]; then
  rm -f "${TRUSTSTORE_LOCATION}"

  create_truststore "$TRUSTSTORE_LOCATION" "${CERTS_STORE_PASSWORD}" \
    "${CA_LOCATION}/ca.crt" "clients-ca"
fi

if [ -n "${KEYSTORE_LOCATION}" ]; then
  rm -f "${KEYSTORE_LOCATION}"

  create_keystore "$KEYSTORE_LOCATION" "${CERTS_STORE_PASSWORD}" \
    "${USER_LOCATION}/user.crt" "${USER_LOCATION}/user.key" \
    "${USER_LOCATION}/ca.crt" "$KAFKA_USER"
fi