#!/bin/bash

export KAFKA_SECURITY="listener.name.replication.ssl.keystore.location=/tmp/kafka/replication.keystore.jks
listener.name.replication.ssl.truststore.location=/tmp/kafka/replication.truststore.jks
listener.name.clienttls.ssl.keystore.location=/tmp/kafka/clients.keystore.jks
listener.name.clienttls.ssl.truststore.location=/tmp/kafka/clients.truststore.jks
ssl.keystore.password=${CERTS_STORE_PASSWORD}
ssl.truststore.password=${CERTS_STORE_PASSWORD}"

# Write the config file
cat <<EOF
broker.id=${KAFKA_BROKER_ID}
broker.rack=${KAFKA_RACK}
# Listeners
listeners=CLIENT://0.0.0.0:9092,REPLICATION://0.0.0.0:9091,CLIENTTLS://0.0.0.0:9093
advertised.listeners=CLIENT://$(hostname -f):9092,REPLICATION://$(hostname -f):9091,CLIENTTLS://$(hostname -f):9093
listener.security.protocol.map=CLIENT:PLAINTEXT,REPLICATION:SSL,CLIENTTLS:SSL
inter.broker.listener.name=REPLICATION
# Zookeeper
zookeeper.connect=${KAFKA_ZOOKEEPER_CONNECT:-zookeeper:2181}
zookeeper.connection.timeout.ms=6000
# Logs
log.dirs=${KAFKA_LOG_DIRS}
# Security
${KAFKA_SECURITY}
# Provided configuration
${KAFKA_CONFIGURATION}
EOF