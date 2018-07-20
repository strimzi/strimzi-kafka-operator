#!/bin/bash

# Parameters:
# $1: Path to the new truststore
# $2: Truststore password
# $3: Public key to be imported
# $4: Alias of the certificate
function create_truststore {
   keytool -keystore $1 -storepass $2 -noprompt -alias $4 -import -file $3 -storetype PKCS12
}

# Parameters:
# $1: Path to the new keystore
# $2: Truststore password
# $3: Public key to be imported
# $4: Private key to be imported
# $5: CA public key to be imported
# $6: Alias of the certificate
function create_keystore {
   RANDFILE=/tmp/.rnd openssl pkcs12 -export -in $3 -inkey $4 -chain -CAfile $5 -name topic-operator -password pass:$2 -out $1
}

echo "CERTS_STORE_PASSWORD=${CERTS_STORE_PASSWORD}"

echo "Preparing certificates for internal communication"
create_truststore /tmp/topic-operator/replication.truststore.p12 $CERTS_STORE_PASSWORD /etc/tls-sidecar/certs/cluster-ca.crt cluster-ca
create_keystore /tmp/topic-operator/replication.keystore.p12 $CERTS_STORE_PASSWORD /etc/tls-sidecar/certs/topic-operator.crt /etc/tls-sidecar/certs/topic-operator.key /etc/tls-sidecar/certs/cluster-ca.crt topic-operator
echo "Preparing certificates for internal communication is complete"
