#!/usr/bin/env bash

# Parameters:
# $1: Broker ID
# $2: List of options
function get_option_for_broker {
  for OPTION in $2 ; do
    if [[ $OPTION == $1://* ]] ; then
      echo ${OPTION#"$1://"}
    fi
  done
}

# get broker rack if it's enabled
if [ -e $KAFKA_HOME/init/rack.id ]; then
  export STRIMZI_RACK_ID=$(cat $KAFKA_HOME/init/rack.id)
fi

# Find the external hostname for this broker passed by the operator
CONFIG_FILE=$KAFKA_HOME/custom-config/advertised-hostnames.config
if [ -e "$CONFIG_FILE" ]; then
  EXTERNAL_9094_ADDRESS_LIST=$(cat $CONFIG_FILE)
  export STRIMZI_EXTERNAL_9094_ADVERTISED_HOSTNAME=$(get_option_for_broker "$STRIMZI_BROKER_ID" "$EXTERNAL_9094_ADDRESS_LIST")
fi

# If there is no external hostname fromthe operator, lets try the init container
NODE_PORT_CONFIG_FILE=$KAFKA_HOME/init/external.address
if [ -z "$STRIMZI_EXTERNAL_9094_ADVERTISED_HOSTNAME" ] && [ -e "$NODE_PORT_CONFIG_FILE" ]; then
  export STRIMZI_EXTERNAL_9094_ADVERTISED_HOSTNAME=$(cat $NODE_PORT_CONFIG_FILE)
fi

# Find the external port for this broker
CONFIG_FILE=$KAFKA_HOME/custom-config/advertised-ports.config
if [ -e "$CONFIG_FILE" ]; then
  EXTERNAL_9094_PORT_LIST=$(cat $CONFIG_FILE)
  export STRIMZI_EXTERNAL_9094_ADVERTISED_PORT=$(get_option_for_broker "$STRIMZI_BROKER_ID" "$EXTERNAL_9094_PORT_LIST")
fi

envsubst \
    '${STRIMZI_BROKER_ID},${STRIMZI_RACK_ID},${CERTS_STORE_PASSWORD},${STRIMZI_PLAIN-9092_OAUTH_CLIENT_SECRET},${STRIMZI_TLS-9093_OAUTH_CLIENT_SECRET},${STRIMZI_EXTERNAL-9094_OAUTH_CLIENT_SECRET},${STRIMZI_EXTERNAL_9094_ADVERTISED_HOSTNAME},${STRIMZI_EXTERNAL_9094_ADVERTISED_PORT}' \
    < $KAFKA_HOME/custom-config/server.config
