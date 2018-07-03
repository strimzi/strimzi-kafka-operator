#!/bin/bash

keytool -keystore /tmp/kafka/clients.truststore.p12 -storepass $CERTS_STORE_PASSWORD -noprompt -alias clients-ca -import -file /opt/kafka/clients-certs/clients-ca.crt -storetype PKCS12
RANDFILE=/tmp/.rnd openssl pkcs12 -export -in /opt/kafka/internal-certs/$HOSTNAME.crt -inkey /opt/kafka/internal-certs/$HOSTNAME.key -chain -CAfile /opt/kafka/internal-certs/internal-ca.crt -name $HOSTNAME -password pass:$CERTS_STORE_PASSWORD -out /tmp/kafka/clients.keystore.p12
