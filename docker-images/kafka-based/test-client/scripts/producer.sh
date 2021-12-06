#!/usr/bin/env bash
set -x

# Defaults
CONFIG_REGEX="USER=([A-Za-z0-9\_]*)"
PRODUCER_CONFIGURATION_ENV="PRODUCER_CONFIGURATION"

# Create options for consumer
if [ -z "$PRODUCER_OPTS" ]; then
  PRODUCER_OPTS_TMP=( "${@}" )
  for i in ${PRODUCER_OPTS_TMP[@]};do
    if [[ ${i} =~ $CONFIG_REGEX ]]
    then
        USER="${BASH_REMATCH[1]}"
        TMP_ENV="PRODUCER_CONFIGURATION_${USER}"
        PRODUCER_CONFIGURATION="${!TMP_ENV}"
        PRODUCER_TLS=$(eval "echo \$$(echo PRODUCER_TLS_${USER})")
        KEYSTORE_LOCATION=$(eval "echo \$$(echo KEYSTORE_LOCATION_${USER})")
        TRUSTSTORE_LOCATION=$(eval "echo \$$(echo TRUSTSTORE_LOCATION_${USER})")
    else
        PRODUCER_OPTS+=("${i}")
    fi
  done
fi

if [ -z "$KAFKA_LOG4J_OPTS" ]; then
  export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$KAFKA_HOME/log4j.properties"
fi

if [ -z "$PRODUCER_OPTS" ]; then
  PRODUCER_OPTS="$@"
fi

# We don't need LOG_DIR because we write no log files, but setting it to a
# directory avoids trying to create it (and logging a permission denied error)
export LOG_DIR="$KAFKA_HOME"

if [ "$PRODUCER_TLS"="TRUE" ]; then
  if [ -z "$CERTS_STORE_PASSWORD" ]; then
    export CERTS_STORE_PASSWORD=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)
  fi
  if [ -n "${KEYSTORE_LOCATION}" ]; then
    PRODUCER_CONFIGURATION="${PRODUCER_CONFIGURATION}
ssl.keystore.password=${CERTS_STORE_PASSWORD}"
  fi
  if [ -n "${TRUSTSTORE_LOCATION}" ]; then
    PRODUCER_CONFIGURATION="${PRODUCER_CONFIGURATION}
ssl.truststore.password=${CERTS_STORE_PASSWORD}"
  fi
  ./kafka_tls_prepare_certificates.sh "${USER}"
fi

PROPERTIES_FILE="/tmp/${USER}.properties"
echo $PROPERTIES_FILE

echo "Starting Producer with configuration:"
echo "${PRODUCER_CONFIGURATION}" | tee ${PROPERTIES_FILE}

. ./set_kafka_gc_options.sh

# starting Kafka server with final configuration
$KAFKA_HOME/bin/kafka-verifiable-producer.sh --producer.config $PROPERTIES_FILE "${PRODUCER_OPTS[@]}"
