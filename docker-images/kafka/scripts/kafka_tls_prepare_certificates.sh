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
   RANDFILE=/tmp/.rnd openssl pkcs12 -export -in $3 -inkey $4 -chain -CAfile $5 -name $6 -password pass:$2 -out $1
}

echo "Preparing truststore for replication listener"
create_truststore /tmp/kafka/cluster.truststore.p12 $CERTS_STORE_PASSWORD /opt/kafka/broker-certs/cluster-ca.crt cluster-ca
echo "Preparing truststore for replication listener is complete"

echo "Preparing keystore for replication and clienttls listener"
create_keystore /tmp/kafka/cluster.keystore.p12 $CERTS_STORE_PASSWORD /opt/kafka/broker-certs/$HOSTNAME.crt /opt/kafka/broker-certs/$HOSTNAME.key /opt/kafka/broker-certs/cluster-ca.crt $HOSTNAME
echo "Preparing keystore for replication and clienttls listener is complete"

echo "Preparing truststore for clienttls listener"
create_truststore /tmp/kafka/clients.truststore.p12 $CERTS_STORE_PASSWORD /opt/kafka/client-ca-cert/ca.crt clients-ca
echo "Preparing truststore for clienttls listener is complete"