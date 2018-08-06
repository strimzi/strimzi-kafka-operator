#!/bin/bash

# Parameters:
# $1: Path to the new truststore
# $2: Truststore password
# $3: Public key to be imported
# $4: Alias of the certificate
function create_truststore {
   keytool -keystore $1 -storepass $2 -noprompt -alias $4 -import -file $3 -storetype PKCS12
}

echo "Preparing truststore"

IFS=';' read -ra CERTS <<< ${KAFKA_CONNECT_TRUSTED_CERTS}
for cert in "${CERTS[@]}"
do
    create_truststore /tmp/kafka/cluster.truststore.p12 $CERTS_STORE_PASSWORD /opt/kafka/trusted-certs/$cert $cert
done
echo "Preparing truststore is complete"