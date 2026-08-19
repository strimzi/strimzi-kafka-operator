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
    PASSWORD=$2 keytool ${KEYTOOL_OPTS} -keystore "$1" -storepass:env PASSWORD -noprompt -alias "$4" -import -file "$3" -storetype PKCS12
}

# Parameters:
# $1: Path to the new keystore
# $2: Truststore password
# $3: Public key to be imported
# $4: Private key to be imported
# $5: CA public key to be imported
# $6: Alias of the certificate
function create_keystore {
    # In FIPS mode openssl defaults the PKCS12 integrity MAC to PBMAC1 (needs PKCS12KDF, which
    # is not FIPS-approved) and the FIPS JVM cannot read it. Skip the MAC instead: the keystore
    # is ephemeral (regenerated in /tmp on every start from read-only secrets, never persisted),
    # so its tamper-detection MAC adds no security here. Key material stays encrypted via -keypbe.
    if [ "$FIPS_MODE" = "disabled" ]; then
        MAC_OPTS="-macalg sha256"
    else
        MAC_OPTS="-nomac"
    fi

    # shellcheck disable=SC2086
    PASSWORD=$2 RANDFILE=/tmp/.rnd openssl pkcs12 -export -in "$3" -inkey "$4" -chain -CAfile "$5" -name "$6" -password env:PASSWORD -out "$1" -certpbe aes-128-cbc -keypbe aes-128-cbc $MAC_OPTS
}

# Parameters:
# $1: Path to the new keystore
# $2: Truststore password
# $3: Public key to be imported
# $4: Private key to be imported
# $5: Alias of the certificate
function create_keystore_without_ca_file {
    # See create_keystore: skip the PKCS12 MAC in FIPS mode (PBMAC1 is unreadable by the FIPS JVM).
    if [ "$FIPS_MODE" = "disabled" ]; then
        MAC_OPTS="-macalg sha256"
    else
        MAC_OPTS="-nomac"
    fi

    # shellcheck disable=SC2086
    PASSWORD=$2 RANDFILE=/tmp/.rnd openssl pkcs12 -export -in "$3" -inkey "$4" -name "$5" -password env:PASSWORD -out "$1" -certpbe aes-128-cbc -keypbe aes-128-cbc $MAC_OPTS
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