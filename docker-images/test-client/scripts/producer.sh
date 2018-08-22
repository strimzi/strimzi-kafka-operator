#!/bin/bash
set -x

# Disable Kafka's GC logging (which logs to a file)...
export GC_LOG_ENABLED="false"
# ... but enable equivalent GC logging to stdout
##export KAFKA_GC_LOG_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps"

if [ -z "$KAFKA_LOG4J_OPTS" ]; then
  export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$KAFKA_HOME/log4j.properties"
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
  ./kafka_tls_prepare_certificates.sh
fi

echo "Starting Producer with configuration:"
echo "${PRODUCER_CONFIGURATION}" | tee /tmp/producer.properties

# starting Kafka server with final configuration
$KAFKA_HOME/bin/kafka-verifiable-producer.sh --producer.config /tmp/producer.properties $PRODUCER_OPTS
