#!/bin/bash

# Write the config file
cat <<EOF
broker.id=${KAFKA_BROKER_ID}
broker.rack=${KAFKA_RACK}
# Listeners
listeners=CLIENT://0.0.0.0:9092,REPLICATION://0.0.0.0:9091
advertised.listeners=CLIENT://$(hostname -f):9092,REPLICATION://$(hostname -f):9091
listener.security.protocol.map=CLIENT:${KAFKA_LISTENER_SECURITY_PROTOCOL:-PLAINTEXT},REPLICATION:${KAFKA_LISTENER_SECURITY_PROTOCOL:-PLAINTEXT}
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