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

    if [ -n "$KAFKA_CONNECT_TLS_AUTH_CERT" ] && [ -n "$KAFKA_CONNECT_TLS_AUTH_KEY" ]; then
        TLS_AUTH_CONFIGURATION=$(cat <<EOF
ssl.keystore.location=/tmp/kafka/cluster.keystore.p12
ssl.keystore.password=${CERTS_STORE_PASSWORD}
ssl.keystore.type=PKCS12

producer.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12
producer.ssl.keystore.password=${CERTS_STORE_PASSWORD}
producer.ssl.keystore.type=PKCS12

consumer.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12
consumer.ssl.keystore.password=${CERTS_STORE_PASSWORD}
consumer.ssl.keystore.type=PKCS12
EOF
)
    fi
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
${TLS_AUTH_CONFIGURATION}
EOF