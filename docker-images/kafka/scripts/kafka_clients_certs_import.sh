#!/bin/bash

keytool -keystore /tmp/kafka/clients.truststore.jks -storepass $CERTS_STORE_PASSWORD -noprompt -alias clients-ca -import -file /opt/kafka/clients-certs/clients-ca.crt
keytool -keystore /tmp/kafka/clients.truststore.jks -storepass $CERTS_STORE_PASSWORD -noprompt -alias internal-ca -import -file /opt/kafka/internal-certs/internal-ca.crt
openssl pkcs12 -export -in /opt/kafka/clients-certs/$HOSTNAME.crt -inkey /opt/kafka/clients-certs/$HOSTNAME.key -chain -CAfile /opt/kafka/clients-certs/clients-ca.crt -name $HOSTNAME -password pass:$CERTS_STORE_PASSWORD -out /tmp/kafka/$HOSTNAME-clients.p12
keytool -importkeystore -deststorepass $CERTS_STORE_PASSWORD -destkeystore /tmp/kafka/clients.keystore.jks -srcstorepass $CERTS_STORE_PASSWORD -srckeystore /tmp/kafka/$HOSTNAME-clients.p12 -srcstoretype PKCS12
