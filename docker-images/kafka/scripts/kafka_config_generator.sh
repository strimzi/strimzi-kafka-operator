#!/bin/bash

# Prepare super.users field
KAFKA_NAME=$(hostname | rev | cut -d "-" -f2- | rev)
ASSEMBLY_NAME=$(echo "${KAFKA_NAME}" | rev | cut -d "-" -f2- | rev)
SUPER_USERS="super.users=User:CN=${ASSEMBLY_NAME}-topic-operator,O=io.strimzi;User:CN=${KAFKA_NAME},O=io.strimzi"

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
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=6000

# Logs
log.dirs=${KAFKA_LOG_DIRS}

# TLS / SSL
ssl.keystore.password=${CERTS_STORE_PASSWORD}
ssl.truststore.password=${CERTS_STORE_PASSWORD}
ssl.keystore.type=PKCS12
ssl.truststore.type=PKCS12

listener.name.replication.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12
listener.name.replication.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12
listener.name.replication.ssl.client.auth=required

listener.name.clienttls.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12
listener.name.clienttls.ssl.truststore.location=/tmp/kafka/clients.truststore.p12

# ACL Super users (all nodes for replication)
${SUPER_USERS}

# Provided configuration
${KAFKA_CONFIGURATION}
EOF