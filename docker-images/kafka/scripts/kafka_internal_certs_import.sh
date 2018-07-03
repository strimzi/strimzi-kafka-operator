#!/bin/bash

keytool -keystore /tmp/kafka/replication.truststore.p12 -storepass $CERTS_STORE_PASSWORD -noprompt -alias internal-ca -import -file /opt/kafka/internal-certs/internal-ca.crt -storetype PKCS12
RANDFILE=/tmp/.rnd openssl pkcs12 -export -in /opt/kafka/internal-certs/$HOSTNAME.crt -inkey /opt/kafka/internal-certs/$HOSTNAME.key -chain -CAfile /opt/kafka/internal-certs/internal-ca.crt -name $HOSTNAME -password pass:$CERTS_STORE_PASSWORD -out /tmp/kafka/replication.keystore.p12
