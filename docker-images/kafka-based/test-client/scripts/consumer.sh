#!/usr/bin/env bash
set -x

. ./set_kafka_gc_options.sh

# Defaults
CONFIG_REGEX="USER=([A-Za-z0-9\_]*)"
CONSUMER_CONFIGURATION_ENV="CONSUMER_CONFIGURATION"

# Create options for consumer
if [ -z "$CONSUMER_OPTS" ]; then
  CONSUMER_OPTS_TMP=( "${@}" )
  for i in ${CONSUMER_OPTS_TMP[@]};do
    if [[ ${i} =~ $CONFIG_REGEX ]]
    then
        USER="${BASH_REMATCH[1]}"
        TMP_ENV="CONSUMER_CONFIGURATION_${USER}"
        CONSUMER_CONFIGURATION=="${!TMP_ENV}"
        CONSUMER_TLS=$(eval "echo \$$(echo CONSUMER_TLS_${USER})")
        KEYSTORE_LOCATION=$(eval "echo \$$(echo KEYSTORE_LOCATION_${USER})")
        TRUSTSTORE_LOCATION=$(eval "echo \$$(echo TRUSTSTORE_LOCATION_${USER})")
    else
        CONSUMER_OPTS+=("${i}")
    fi
  done
fi

if [ -z "$KAFKA_LOG4J_OPTS" ]; then
  export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$KAFKA_HOME/log4j.properties"
fi

# We don't need LOG_DIR because we write no log files, but setting it to a
# directory avoids trying to create it (and logging a permission denied error)
export LOG_DIR="$KAFKA_HOME"

CONSUMER_CONFIGURATION="${!CONSUMER_CONFIGURATION_ENV}"

if [ "$CONSUMER_TLS"="TRUE" ]; then
  if [ -z "$CERTS_STORE_PASSWORD" ]; then
    export CERTS_STORE_PASSWORD=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)
  fi
  if [ -n "${KEYSTORE_LOCATION}" ]; then
    CONSUMER_CONFIGURATION="${CONSUMER_CONFIGURATION}
ssl.keystore.password=${CERTS_STORE_PASSWORD}"
  fi
  if [ -n "${TRUSTSTORE_LOCATION}" ]; then
    CONSUMER_CONFIGURATION="${CONSUMER_CONFIGURATION}
ssl.truststore.password=${CERTS_STORE_PASSWORD}"
  fi
  ./kafka_tls_prepare_certificates.sh "${USER}"
fi

PROPERTIES_FILE="/tmp/${USER}.properties"
echo $PROPERTIES_FILE

# Generate config file for consumer
echo "Starting Consumer with configuration:"
echo "${CONSUMER_CONFIGURATION}" | tee ${PROPERTIES_FILE}

# starting Kafka server with final configuration
$KAFKA_HOME/bin/kafka-verifiable-consumer.sh --consumer.config $PROPERTIES_FILE "${CONSUMER_OPTS[@]}"

RET=$?

echo $RET

exit $RET
