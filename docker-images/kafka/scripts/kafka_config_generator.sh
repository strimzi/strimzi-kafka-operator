#!/bin/bash

#####
# PLAIN listener
#####
LISTENERS="REPLICATION://0.0.0.0:9091"
ADVERTISED_LISTENERS="REPLICATION://$(hostname -f):9091"
LISTENER_SECURITY_PROTOCOL_MAP="REPLICATION:SSL"
SASL_ENABLED_MECHANISMS=""

if [ "$KAFKA_CLIENT_ENABLED" = "TRUE" ]; then
  LISTENERS="${LISTENERS},CLIENT://0.0.0.0:9092"
  ADVERTISED_LISTENERS="${ADVERTISED_LISTENERS},CLIENT://$(hostname -f):9092"

  if [ "$KAFKA_CLIENT_AUTHENTICATION" = "scram-sha-512" ]; then
    SASL_ENABLED_MECHANISMS="SCRAM-SHA-512\n$SASL_ENABLED_MECHANISMS"
    LISTENER_SECURITY_PROTOCOL_MAP="${LISTENER_SECURITY_PROTOCOL_MAP},CLIENT:SASL_PLAINTEXT"
    CLIENT_LISTENER=$(cat <<EOF
# CLIENT listener authentication
listener.name.client.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
EOF
)
  else
    LISTENER_SECURITY_PROTOCOL_MAP="${LISTENER_SECURITY_PROTOCOL_MAP},CLIENT:PLAINTEXT"
  fi
fi

#####
# TLS listener
#####
if [ "$KAFKA_CLIENTTLS_ENABLED" = "TRUE" ]; then
  LISTENERS="${LISTENERS},CLIENTTLS://0.0.0.0:9093"
  ADVERTISED_LISTENERS="${ADVERTISED_LISTENERS},CLIENTTLS://$(hostname -f):9093"

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
# CLIENTTLS listener authentication
listener.name.clienttls.ssl.client.auth=${LISTENER_NAME_CLIENTTLS_SSL_CLIENT_AUTH}
EOF
)

  if [ "$KAFKA_CLIENTTLS_AUTHENTICATION" = "scram-sha-512" ]; then
    SASL_ENABLED_MECHANISMS="SCRAM-SHA-512\n$SASL_ENABLED_MECHANISMS"
    LISTENER_SECURITY_PROTOCOL_MAP="${LISTENER_SECURITY_PROTOCOL_MAP},CLIENTTLS:SASL_SSL"
    CLIENTTLS_LISTENER=$(cat <<EOF
$CLIENTTLS_LISTENER
# CLIENTTLS listener authentication
listener.name.clienttls.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
EOF
)
  else
    LISTENER_SECURITY_PROTOCOL_MAP="${LISTENER_SECURITY_PROTOCOL_MAP},CLIENTTLS:SSL"
  fi
fi

#####
# EXTERNAL listener
#####
if [ "$KAFKA_EXTERNAL_ENABLED" ]; then
  LISTENERS="${LISTENERS},EXTERNAL://0.0.0.0:9094"
  ADDRESSES=($KAFKA_EXTERNAL_ADDRESSES)

  if [ "$KAFKA_EXTERNAL_ENABLED" = "route" ]; then
    ADVERTISED_LISTENERS="${ADVERTISED_LISTENERS},EXTERNAL://${ADDRESSES[$KAFKA_BROKER_ID]}:443"
  elif [ "$KAFKA_EXTERNAL_ENABLED" = "loadbalancer" ]; then
    ADVERTISED_LISTENERS="${ADVERTISED_LISTENERS},EXTERNAL://${ADDRESSES[$KAFKA_BROKER_ID]}:9094"
  elif [ "$KAFKA_EXTERNAL_ENABLED" = "nodeport" ]; then
    if [ -e $KAFKA_HOME/init/external.address ]; then
      EXTERNAL_ADDRESS=$(cat $KAFKA_HOME/init/external.address)
    else
      echo "-E- External address not found"
      exit 1
    fi

    ADVERTISED_LISTENERS="${ADVERTISED_LISTENERS},EXTERNAL://${EXTERNAL_ADDRESS}:${ADDRESSES[$KAFKA_BROKER_ID]}"
  fi

  if [ "$KAFKA_EXTERNAL_TLS" = "true" ]; then
    # Configuring TLS client authentication for clienttls interface
    if [ "$KAFKA_EXTERNAL_AUTHENTICATION" = "tls" ]; then
      LISTENER_NAME_EXTERNAL_SSL_CLIENT_AUTH="required"
    else
      LISTENER_NAME_EXTERNAL_SSL_CLIENT_AUTH="none"
    fi

    EXTERNAL_LISTENER=$(cat <<EOF
# EXTERNAL interface configuration
listener.name.external.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12
listener.name.external.ssl.truststore.location=/tmp/kafka/clients.truststore.p12
# EXTERNAL listener authentication
listener.name.external.ssl.client.auth=${LISTENER_NAME_EXTERNAL_SSL_CLIENT_AUTH}
EOF
)
  fi

  if [ "$KAFKA_EXTERNAL_AUTHENTICATION" = "scram-sha-512" ]; then
    SASL_ENABLED_MECHANISMS="SCRAM-SHA-512\n$SASL_ENABLED_MECHANISMS"

    if [ "$KAFKA_EXTERNAL_TLS" = "true" ]; then
      LISTENER_SECURITY_PROTOCOL_MAP="${LISTENER_SECURITY_PROTOCOL_MAP},EXTERNAL:SASL_SSL"
    else
      LISTENER_SECURITY_PROTOCOL_MAP="${LISTENER_SECURITY_PROTOCOL_MAP},EXTERNAL:SASL_PLAINTEXT"
    fi

    EXTERNAL_LISTENER=$(cat <<EOF
$EXTERNAL_LISTENER
# EXTERNAL listener authentication
listener.name.external.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
EOF
)
  else
    if [ "$KAFKA_EXTERNAL_TLS" = "true" ]; then
      LISTENER_SECURITY_PROTOCOL_MAP="${LISTENER_SECURITY_PROTOCOL_MAP},EXTERNAL:SSL"
    else
      LISTENER_SECURITY_PROTOCOL_MAP="${LISTENER_SECURITY_PROTOCOL_MAP},EXTERNAL:PLAINTEXT"
    fi
  fi
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
ssl.secure.random.implementation=SHA1PRNG

listener.name.replication.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12
listener.name.replication.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12
listener.name.replication.ssl.client.auth=required

sasl.enabled.mechanisms=$(echo -e "$SASL_ENABLED_MECHANISMS" | uniq | awk -vORS=, '/.+/{ print $1 }' | sed 's/,$/\n/')

${CLIENT_LISTENER}
${CLIENTTLS_LISTENER}
${EXTERNAL_LISTENER}

# Authorization configuration
authorizer.class.name=${AUTHORIZER_CLASS_NAME}
${SUPER_USERS}

# Provided configuration
${KAFKA_CONFIGURATION}
EOF