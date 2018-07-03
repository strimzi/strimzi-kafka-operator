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
   RANDFILE=/tmp/.rnd openssl pkcs12 -export -in $3 -inkey $4 -chain -CAfile $5 -name $HOSTNAME -password pass:$2 -out $1
}

echo "Preparing certificates for internal communication"
create_truststore /tmp/kafka/replication.truststore.p12 $CERTS_STORE_PASSWORD /opt/kafka/internal-certs/internal-ca.crt internal-ca
create_keystore /tmp/kafka/replication.keystore.p12 $CERTS_STORE_PASSWORD /opt/kafka/internal-certs/$HOSTNAME.crt /opt/kafka/internal-certs/$HOSTNAME.key /opt/kafka/internal-certs/internal-ca.crt $HOSTNAME
echo "Preparing certificates for internal communication is complete"

echo "Preparing certificates for clients communication"
create_truststore /tmp/kafka/clients.truststore.p12 $CERTS_STORE_PASSWORD /opt/kafka/clients-certs/internal-ca.crt clients-ca
create_keystore /tmp/kafka/clients.keystore.p12 $CERTS_STORE_PASSWORD /opt/kafka/clients-certs/$HOSTNAME.crt /opt/kafka/clients-certs/$HOSTNAME.key /opt/kafka/clients-certs/clients-ca.crt $HOSTNAME
echo "Preparing certificates for clients communication is complete"