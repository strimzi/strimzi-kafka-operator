#!/bin/bash

#####
# Configuring listeners
#####
LISTENERS="REPLICATION://0.0.0.0:9091"
ADVERTISED_LISTENERS="REPLICATION://$(hostname -f):9091"
LISTENER_SECURITY_PROTOCOL_MAP="REPLICATION:SSL"

if [ "$KAFKA_CLIENT_ENABLED" = "TRUE" ]; then
  LISTENERS="${LISTENERS},CLIENT://0.0.0.0:9092"
  ADVERTISED_LISTENERS="${ADVERTISED_LISTENERS},CLIENT://$(hostname -f):9092"
  LISTENER_SECURITY_PROTOCOL_MAP="${LISTENER_SECURITY_PROTOCOL_MAP},CLIENT:PLAINTEXT"
fi

if [ "$KAFKA_CLIENTTLS_ENABLED" = "TRUE" ]; then
  LISTENERS="${LISTENERS},CLIENTTLS://0.0.0.0:9093"
  ADVERTISED_LISTENERS="${ADVERTISED_LISTENERS},CLIENTTLS://$(hostname -f):9093"
  LISTENER_SECURITY_PROTOCOL_MAP="${LISTENER_SECURITY_PROTOCOL_MAP},CLIENTTLS:SSL"

  # Configuring TLS client authentication for clienttls interface
  if [ "$KAFKA_CLIENTTLS_AUTHENTICATION" = "tls" ]; then
    LISTENER_NAME_CLIENTTLS_SSL_CLIENT_AUTH="required"
  else
    LISTENER_NAME_CLIENTTLS_SSL_CLIENT_AUTH="none"
  fi

  CLIENTTLS_LISTENER=$(cat <<EOF
# TLS interface configuration
listener.name.clienttls.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12
listener.name.clienttls.ssl.truststore.location=/tmp/kafka/clients.truststore.p12
listener.name.clienttls.ssl.client.auth=${LISTENER_NAME_CLIENTTLS_SSL_CLIENT_AUTH}
EOF
)
fi

#####
# Configuring authorization
#####
if [ "$KAFKA_AUTHORIZATION_TYPE" = "simple" ]; then
  AUTHORIZER_CLASS_NAME="kafka.security.auth.SimpleAclAuthorizer"

  # Prepare super.users field
  KAFKA_NAME=$(hostname | rev | cut -d "-" -f2- | rev)
  ASSEMBLY_NAME=$(echo "${KAFKA_NAME}" | rev | cut -d "-" -f2- | rev)
  SUPER_USERS="super.users=User:CN=${KAFKA_NAME},O=io.strimzi;User:CN=${ASSEMBLY_NAME}-entity-operator,O=io.strimzi"

  if [ "$KAFKA_AUTHORIZATION_SUPER_USERS" ]; then
    SUPER_USERS="${SUPER_USERS};${KAFKA_AUTHORIZATION_SUPER_USERS}"
  fi
else
  AUTHORIZER_CLASS_NAME=""
fi

# Write the config file
cat <<EOF
broker.id=${KAFKA_BROKER_ID}
broker.rack=${KAFKA_RACK}

# Listeners
listeners=${LISTENERS}
advertised.listeners=${ADVERTISED_LISTENERS}
listener.security.protocol.map=${LISTENER_SECURITY_PROTOCOL_MAP}
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
ssl.endpoint.identification.algorithm=HTTPS

listener.name.replication.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12
listener.name.replication.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12
listener.name.replication.ssl.client.auth=required

${CLIENTTLS_LISTENER}

# Authorization configuration
authorizer.class.name=${AUTHORIZER_CLASS_NAME}
${SUPER_USERS}

# Provided configuration
${KAFKA_CONFIGURATION}
EOF