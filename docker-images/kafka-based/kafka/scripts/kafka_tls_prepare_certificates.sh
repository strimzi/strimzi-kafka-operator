#!/usr/bin/env bash
set -e

# Load predefined functions for preparing trust- and keystores
source ./tls_utils.sh

regex="^\/opt\/kafka\/certificates\/(oauth)-(.+)-(.+)-certs$"
for CERT_DIR in /opt/kafka/certificates/*; do
  if [[ $CERT_DIR =~ $regex ]]; then
    listener=${BASH_REMATCH[1]}-${BASH_REMATCH[2]}-${BASH_REMATCH[3]}
    echo "Preparing store for $listener oauth listener"
    if [[ ${BASH_REMATCH[1]} == "oauth"  ]]; then
      trusted_certs="STRIMZI_${BASH_REMATCH[2]^^}_${BASH_REMATCH[3]}_OAUTH_TRUSTED_CERTS"
      if [ -n "${!trusted_certs}" ]; then
        prepare_truststore "/tmp/kafka/$listener.truststore.p12" "$CERTS_STORE_PASSWORD" "$CERT_DIR" "${!trusted_certs}"
      fi
    fi
    echo "Preparing store for ${BASH_REMATCH[1]} ${BASH_REMATCH[2]} listener is complete"  
  fi
done

if [ -n "$STRIMZI_OPA_AUTHZ_TRUSTED_CERTS" ]; then
  echo "Preparing Open Policy Agent authorization truststore"
  prepare_truststore "/tmp/kafka/authz-opa.truststore.p12" "$CERTS_STORE_PASSWORD" "/opt/kafka/certificates/authz-opa-certs" "$STRIMZI_OPA_AUTHZ_TRUSTED_CERTS"
fi

if [ -n "$STRIMZI_KEYCLOAK_AUTHZ_TRUSTED_CERTS" ]; then
  echo "Preparing Keycloak authorization truststore"
  prepare_truststore "/tmp/kafka/authz-keycloak.truststore.p12" "$CERTS_STORE_PASSWORD" "/opt/kafka/certificates/authz-keycloak-certs" "$STRIMZI_KEYCLOAK_AUTHZ_TRUSTED_CERTS"
fi
