#!/bin/bash

keytool -keystore /tmp/kafka/replication.truststore.jks -storepass $CERTS_STORE_PASSWORD -noprompt -alias internal-ca -import -file /opt/kafka/internal-certs/internal-ca.crt
openssl pkcs12 -export -in /opt/kafka/internal-certs/$HOSTNAME.crt -inkey /opt/kafka/internal-certs/$HOSTNAME.key -chain -CAfile /opt/kafka/internal-certs/internal-ca.crt -name $HOSTNAME -password pass:$CERTS_STORE_PASSWORD -out /tmp/kafka/$HOSTNAME-internal.p12
keytool -importkeystore -deststorepass $CERTS_STORE_PASSWORD -destkeystore /tmp/kafka/replication.keystore.jks -srcstorepass $CERTS_STORE_PASSWORD -srckeystore /tmp/kafka/$HOSTNAME-internal.p12 -srcstoretype PKCS12
