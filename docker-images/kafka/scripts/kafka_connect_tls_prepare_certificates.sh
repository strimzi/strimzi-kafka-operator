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
# $5: Alias of the certificate
function create_keystore {
   RANDFILE=/tmp/.rnd openssl pkcs12 -export -in "$3" -inkey "$4" -name "$5" -password pass:"$2" -out "$1"
}

echo "Preparing truststore"
IFS=';' read -ra CERTS <<< "${KAFKA_CONNECT_TRUSTED_CERTS}"
for cert in "${CERTS[@]}"
do
    create_truststore /tmp/kafka/cluster.truststore.p12 "$CERTS_STORE_PASSWORD" "/opt/kafka/connect-certs/$cert" "$cert"
done
echo "Preparing truststore is complete"

if [ -n "$KAFKA_CONNECT_TLS_AUTH_CERT" ] && [ -n "$KAFKA_CONNECT_TLS_AUTH_KEY" ]; then
    echo "Preparing keystore"
    create_keystore /tmp/kafka/cluster.keystore.p12 "$CERTS_STORE_PASSWORD" "/opt/kafka/connect-certs/${KAFKA_CONNECT_TLS_AUTH_CERT}" "/opt/kafka/connect-certs/${KAFKA_CONNECT_TLS_AUTH_KEY}" "${KAFKA_CONNECT_TLS_AUTH_CERT}"
    echo "Preparing keystore is complete"
fi

if [ -d /opt/kafka/oauth-certs ]; then
  echo "Preparing truststore for OAuth"
  # Add each certificate to the trust store
  STORE=/tmp/kafka/oauth.truststore.p12
  declare -i INDEX=0
  for CRT in /opt/kafka/oauth-certs/**/*; do
    ALIAS="oauth-${INDEX}"
    echo "Adding $CRT to truststore $STORE with alias $ALIAS"
    create_truststore "$STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
    INDEX+=1
  done
  echo "Preparing truststore for OAuth is complete"
fi