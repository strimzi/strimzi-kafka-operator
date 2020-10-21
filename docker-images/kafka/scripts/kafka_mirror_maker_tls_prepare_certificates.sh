#!/usr/bin/env bash
set -e

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
# $5: Alias of the certificate
function create_keystore {
   RANDFILE=/tmp/.rnd openssl pkcs12 -export -in "$3" -inkey "$4" -name "$5" -password pass:"$2" -out "$1"
}

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
    IFS=';' read -ra CERTS <<< "${trusted_certs}"
    for cert in "${CERTS[@]}"
    do
        create_truststore "$truststore_path" "$CERTS_STORE_PASSWORD" "$certs_key_path/$cert" "$cert"
    done
    echo "Preparing truststore is complete"
fi

if [ -n "$tls_auth_cert" ] && [ -n "$tls_auth_key" ]; then
    echo "Preparing keystore"
    create_keystore "$keystore_path" "$CERTS_STORE_PASSWORD" "$certs_key_path/$tls_auth_cert" "$certs_key_path/$tls_auth_key" "$tls_auth_cert"
    echo "Preparing keystore is complete"
fi

if [ -d "$oauth_certs_path" ]; then
  echo "Preparing truststore for OAuth"
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
