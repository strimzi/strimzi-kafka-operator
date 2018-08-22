#!/bin/bash

set -x

ls -l "${CA_LOCATION}"
ls -l "${USER_LOCATION}"

# Parameters:
# $1: Path to the new truststore
# $2: Truststore password
# $3: Public key to be imported
# $4: Alias of the certificate
function create_truststore {
   keytool -keystore "$1" -storepass "$2" -noprompt -alias "$4" -import -file "$3" -storetype PKCS12
}

rm "${TRUSTSTORE_LOCATION}"

create_truststore "$TRUSTSTORE_LOCATION" "$CERTS_STORE_PASSWORD" \
  "${CA_LOCATION}/ca.crt" "clients-ca"

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

rm "${KEYSTORE_LOCATION}"

create_keystore "$KEYSTORE_LOCATION" "$CERTS_STORE_PASSWORD" \
   "${USER_LOCATION}/user.crt" "${USER_LOCATION}/user.key" \
   "${USER_LOCATION}/ca.crt" "$KAFKA_USER"
