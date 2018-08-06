#!/bin/bash

if [ -n "$KAFKA_CONNECT_TRUSTED_CERTS" ]; then

    TLS_CONFIGURATION=$(cat <<EOF
# TLS / SSL
security.protocol=SSL
ssl.truststore.location=/tmp/kafka/cluster.truststore.p12
ssl.truststore.password=${CERTS_STORE_PASSWORD}
ssl.truststore.type=PKCS12

producer.security.protocol=SSL
producer.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12
producer.ssl.truststore.password=${CERTS_STORE_PASSWORD}

consumer.security.protocol=SSL
consumer.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12
consumer.ssl.truststore.password=${CERTS_STORE_PASSWORD}
EOF
)
fi

# Write the config file
cat <<EOF
# Bootstrap servers
bootstrap.servers=${KAFKA_CONNECT_BOOTSTRAP_SERVERS}
# REST Listeners
rest.port=8083
rest.advertised.host.name=$(hostname -I)
rest.advertised.port=8083
# Plugins
plugin.path=${KAFKA_CONNECT_PLUGIN_PATH}
# Provided configuration
${KAFKA_CONNECT_CONFIGURATION}

${TLS_CONFIGURATION}
EOF