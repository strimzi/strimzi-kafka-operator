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
# $5: Alias of the certificate
function create_keystore {
   RANDFILE=/tmp/.rnd openssl pkcs12 -export -in $3 -inkey $4 -name $5 -password pass:$2 -out $1
}

if [ -n "$KAFKA_MIRRORMAKER_TRUSTED_CERTS_CONSUMER" ]; then
    echo "Preparing consumer truststore"
    IFS=';' read -ra CERTS <<< ${KAFKA_MIRRORMAKER_TRUSTED_CERTS_CONSUMER}
    for cert in "${CERTS[@]}"
    do
        create_truststore /tmp/kafka/consumer.truststore.p12 $CERTS_STORE_PASSWORD /opt/kafka/consumer-certs/$cert $cert
    done
    echo "Preparing consumer truststore is complete"
fi

if [ -n "$KAFKA_MIRRORMAKER_TRUSTED_CERTS_PRODUCER" ]; then
    echo "Preparing producer truststore"
    IFS=';' read -ra CERTS <<< ${KAFKA_MIRRORMAKER_TRUSTED_CERTS_PRODUCER}
    for cert in "${CERTS[@]}"
    do
        create_truststore /tmp/kafka/producer.truststore.p12 $CERTS_STORE_PASSWORD /opt/kafka/producer-certs/$cert $cert
    done
    echo "Preparing producer truststore is complete"
fi


if [ -n "$KAFKA_MIRRORMAKER_TLS_AUTH_CERT_CONSUMER" ] && [ -n "$KAFKA_MIRRORMAKER_TLS_AUTH_KEY_CONSUMER" ]; then
    echo "Preparing consumer keystore"
    create_keystore /tmp/kafka/consumer.keystore.p12 $CERTS_STORE_PASSWORD /opt/kafka/consumer-certs/${KAFKA_MIRRORMAKER_TLS_AUTH_CERT_CONSUMER} /opt/kafka/consumer-certs/${KAFKA_MIRRORMAKER_TLS_AUTH_KEY_CONSUMER} ${KAFKA_MIRRORMAKER_TLS_AUTH_CERT_CONSUMER}
    echo "Preparing consumer keystore is complete"
fi

if [ -n "$KAFKA_MIRRORMAKER_TLS_AUTH_CERT_PRODUCER" ] && [ -n "$KAFKA_MIRRORMAKER_TLS_AUTH_KEY_PRODUCER" ]; then
    echo "Preparing producer keystore"
    create_keystore /tmp/kafka/producer.keystore.p12 $CERTS_STORE_PASSWORD /opt/kafka/producer-certs/${KAFKA_MIRRORMAKER_TLS_AUTH_CERT_PRODUCER} /opt/kafka/producer-certs/${KAFKA_MIRRORMAKER_TLS_AUTH_KEY_PRODUCER} ${KAFKA_MIRRORMAKER_TLS_AUTH_CERT_PRODUCER}
    echo "Preparing producer keystore is complete"
fi