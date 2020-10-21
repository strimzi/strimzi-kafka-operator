#!/usr/bin/env bash
set -e
set +x

# Parameters:
# $1: Path to the new truststore
# $2: Truststore password
# $3: Public key to be imported
# $4: Alias of the certificate
function create_truststore {
  cacerts_path=$(find /usr/lib/jvm -name cacerts)
  cat $cacerts_path > "$1"

  keytool -keystore "$1" \
      -alias "$4" \
      -import \
      -file "$3" \
      -keypass changeit \
      -storepass changeit \
      -noprompt

  # if CA certificate is chained, only import root cert into trustore
  FULLCHAIN=$(<$3)
  ROOT_CERT=$(echo -e "${FULLCHAIN#*-----END CERTIFICATE-----}" | sed '/./,$!d')
  
  if [[ ! -z "${ROOT_CERT// }" ]] ; then
    RND_FILE=$(< /dev/urandom tr -dc _A-Z-a-z | head -c12)
    echo "$ROOT_CERT" > /tmp/$RND_FILE.crt
    
    keytool -keystore "$1" \
      -alias rootCA \
      -import \
      -file /tmp/$RND_FILE.crt \
      -keypass changeit \
      -storepass changeit \
      -noprompt
  fi

  # change default truststore password
  keytool -storepasswd -storepass "changeit" -new "$2" -keystore "$1"
    
}

# Parameters:
# $1: Path to the new keystore
# $2: Truststore password
# $3: Public key to be imported
# $4: Private key to be imported
# $5: CA public key to be imported
# $6: Alias of the certificate
function create_keystore {
   RANDFILE=/tmp/.rnd openssl pkcs12 -export -in "$3" -inkey "$4" -name "$6" -password pass:"$2" -out "$1"
}

echo "Preparing certificates for internal communication"
STORE=/tmp/cruise-control/replication.truststore.p12
for CRT in /etc/tls-sidecar/cluster-ca-certs/*.crt; do
  ALIAS=$(basename "$CRT" .crt)
  echo "Adding $CRT to truststore $STORE with alias $ALIAS"
  create_truststore "$STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
done

create_keystore /tmp/cruise-control/replication.keystore.p12 "$CERTS_STORE_PASSWORD" \
    /etc/tls-sidecar/cc-certs/cruise-control.crt \
    /etc/tls-sidecar/cc-certs/cruise-control.key \
    /etc/tls-sidecar/cluster-ca-certs/ca.crt \
    cruise-control
echo "Preparing certificates for internal communication is complete"
