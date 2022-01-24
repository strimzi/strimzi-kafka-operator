#!/usr/bin/env bash
set -e

# Load predefined functions for preparing trust- and keystores
source ./tls_utils.sh

# $1 = trusted certs, $2 = TLS auth cert, $3 = TLS auth key, $4 = truststore path, $5 = keystore path, $6 = certs and key path
trusted_certs="$1"
tls_auth_cert="$2"
tls_auth_key="$3"
truststore_path="$4"
keystore_path="$5"
certs_key_path="$6"
oauth_certs_path="$7"
oauth_keystore_path="$8"

if [ -n "$trusted_certs" ]; then
    echo "Preparing truststore"
    rm -f "$truststore_path"
    IFS=';' read -ra CERTS <<< "${trusted_certs}"
    for cert in "${CERTS[@]}"
    do
        create_truststore "$truststore_path" "$CERTS_STORE_PASSWORD" "$certs_key_path/$cert" "$cert"
    done
    echo "Preparing truststore is complete"
fi

if [ -n "$tls_auth_cert" ] && [ -n "$tls_auth_key" ]; then
    echo "Preparing keystore"
    rm -f "$keystore_path"
    create_keystore_without_ca_file "$keystore_path" "$CERTS_STORE_PASSWORD" "$certs_key_path/$tls_auth_cert" "$certs_key_path/$tls_auth_key" "$tls_auth_cert"
    echo "Preparing keystore is complete"
fi

if [ -d "$oauth_certs_path" ]; then
  echo "Preparing truststore for OAuth"
  rm -f "$oauth_keystore_path"
  # Add each certificate to the trust store
  declare -i INDEX=0
  for CRT in "$oauth_certs_path"/**/*; do
    ALIAS="oauth-${INDEX}"
    echo "Adding $CRT to truststore $oauth_keystore_path with alias $ALIAS"
    create_truststore "$oauth_keystore_path" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
    INDEX+=1
  done
  echo "Preparing truststore for OAuth is complete"
fi