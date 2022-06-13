#!/usr/bin/env bash
set -e
set +x

# Parameters:
# $1: Path to the new truststore
# $2: Truststore password
# $3: Public key to be imported
# $4: Alias of the certificate
function create_truststore {
    # Disable FIPS if needed
    if [ "$FIPS_MODE" = "disabled" ]; then
        KEYTOOL_OPTS="${KEYTOOL_OPTS} -J-Dcom.redhat.fips=false"
    else
        KEYTOOL_OPTS=""
    fi

    # shellcheck disable=SC2086
    keytool ${KEYTOOL_OPTS} -keystore "$1" -storepass "$2" -noprompt -alias "$4" -import -file "$3" -storetype PKCS12
}

# Parameters:
# $1: Path to the new keystore
# $2: Truststore password
# $3: Public key to be imported
# $4: Private key to be imported
# $5: Alias of the certificate
function create_keystore_without_ca_file {
   RANDFILE=/tmp/.rnd openssl pkcs12 -export -in "$3" -inkey "$4" -name "$5" -password pass:"$2" -out "$1" -certpbe aes-128-cbc -keypbe aes-128-cbc -macalg sha256
}

if [ "$STRIMZI_PUBLIC_CA" != "true" ]; then
    echo "Preparing trust store certificates for internal communication"
    STORE=/tmp/topic-operator/replication.truststore.p12
    rm -f "$STORE"
    for CRT in /etc/tls-sidecar/cluster-ca-certs/*.crt; do
      ALIAS=$(basename "$CRT" .crt)
      echo "Adding $CRT to truststore $STORE with alias $ALIAS"
      create_truststore "$STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
    done
    echo "Preparing trust store certificates for internal communication is completed"
fi

if [ "$STRIMZI_TLS_AUTH_ENABLED" != "false" ]; then
  echo "Preparing key store certificates for internal communication"
  STORE=/tmp/topic-operator/replication.keystore.p12
  rm -f "$STORE"
  create_keystore_without_ca_file "$STORE" "$CERTS_STORE_PASSWORD" \
      /etc/eto-certs/entity-operator.crt \
      /etc/eto-certs/entity-operator.key \
      /etc/tls-sidecar/cluster-ca-certs/ca.crt \
      entity-operator
  echo "Preparing key store certificates for internal communication is completed"
fi
