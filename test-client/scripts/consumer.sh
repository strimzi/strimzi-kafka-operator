#!/usr/bin/env bash
set -x

. ./set_kafka_gc_options.sh

if [ -z "$KAFKA_LOG4J_OPTS" ]; then
  export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$KAFKA_HOME/log4j.properties"
fi

if [ -z "$CONSUMER_OPTS" ]; then
  CONSUMER_OPTS="$@"
fi

# We don't need LOG_DIR because we write no log files, but setting it to a
# directory avoids trying to create it (and logging a permission denied error)
export LOG_DIR="$KAFKA_HOME"

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
  ./kafka_tls_prepare_certificates.sh
fi

echo "Starting Consumer with configuration:"
echo "${CONSUMER_CONFIGURATION}" | tee /tmp/consumer.properties

# starting Kafka server with final configuration
$KAFKA_HOME/bin/kafka-verifiable-consumer.sh --consumer.config /tmp/consumer.properties $CONSUMER_OPTS

RET=$?

echo $RET

exit $RET
