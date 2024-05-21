#!/usr/bin/env bash
set -e

##########
# This file contains bash functions loaded and used by other scripts
##########

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
# $5: CA public key to be imported
# $6: Alias of the certificate
function create_keystore {
   RANDFILE=/tmp/.rnd openssl pkcs12 -export -in "$3" -inkey "$4" -chain -CAfile "$5" -name "$6" -password pass:"$2" -out "$1" -certpbe aes-128-cbc -keypbe aes-128-cbc -macalg sha256
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

# Searches the directory with the CAs and finds the CA matching our key.
# This is useful during certificate renewals
#
# Parameters:
# $1: The directory with the CA certificates
# $2: Public key to be imported
function find_ca {
    for ca in "$1"/*; do
        if openssl verify -CAfile "$ca" "$2" &> /dev/null; then
            echo "$ca"
        fi
    done
}

# Parameters:
# $1: Path to the new truststore
# $2: Truststore password
# $3: Base path where the certificates are mounted
# $4: Environment variable defining the certs that should be loaded
function prepare_truststore {
    TRUSTSTORE=$1
    PASSWORD=$2
    BASEPATH=$3
    TRUSTED_CERTS=$4

    rm -f "$TRUSTSTORE"

    IFS=';' read -ra CERTS <<< "${TRUSTED_CERTS}"
    for cert in "${CERTS[@]}"
    do
        for file in $BASEPATH/$cert
        do
            if [ -f "$file" ]; then
                echo "Adding $file to truststore $TRUSTSTORE with alias $file"
                create_truststore "$TRUSTSTORE" "$PASSWORD" "$file" "$file"
            fi
        done
    done
}