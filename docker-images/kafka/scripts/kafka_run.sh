#!/usr/bin/env bash
set -e
set -x

STRIMZI_BROKER_ID=$(hostname | awk -F'-' '{print $NF}')
export STRIMZI_BROKER_ID
echo "STRIMZI_BROKER_ID=${STRIMZI_BROKER_ID}"

# Disable Kafka's GC logging (which logs to a file)...
export GC_LOG_ENABLED="false"

if [ -z "$KAFKA_LOG4J_OPTS" ]; then
  export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$KAFKA_HOME/custom-config/log4j.properties"
fi

rm -f /var/opt/kafka/kafka-ready /var/opt/kafka/zk-connected 2> /dev/null
KAFKA_OPTS="$KAFKA_OPTS -javaagent:$(ls "$KAFKA_HOME"/libs/kafka-agent*.jar)=/var/opt/kafka/kafka-ready:/var/opt/kafka/zk-connected"
export KAFKA_OPTS

. ./set_kafka_jmx_options.sh "${KAFKA_JMX_ENABLED}" "${KAFKA_JMX_USERNAME}" "${KAFKA_JMX_PASSWORD}"

if [ -n "$STRIMZI_JAVA_SYSTEM_PROPERTIES" ]; then
    export KAFKA_OPTS="${KAFKA_OPTS} ${STRIMZI_JAVA_SYSTEM_PROPERTIES}"
fi

# enabling Prometheus JMX exporter as Java agent
if [ "$KAFKA_METRICS_ENABLED" = "true" ]; then
  KAFKA_OPTS="${KAFKA_OPTS} -javaagent:$(ls "$KAFKA_HOME"/libs/jmx_prometheus_javaagent*.jar)=9404:$KAFKA_HOME/custom-config/metrics-config.yml"
  export KAFKA_OPTS
fi

# We don't need LOG_DIR because we write no log files, but setting it to a
# directory avoids trying to create it (and logging a permission denied error)
export LOG_DIR="$KAFKA_HOME"

# Generate temporary keystore password
CERTS_STORE_PASSWORD=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)
export CERTS_STORE_PASSWORD

mkdir -p /tmp/kafka

# Import certificates into keystore and truststore
./kafka_tls_prepare_certificates.sh

# Generate and print the config file
echo "Starting Kafka with configuration:"
./kafka_config_generator.sh | tee /tmp/strimzi.properties | sed -e 's/sasl.jaas.config=.*/sasl.jaas.config=[hidden]/g' -e 's/password=.*/password=[hidden]/g'
echo ""

if [ -z "$KAFKA_HEAP_OPTS" ] && [ -n "${DYNAMIC_HEAP_FRACTION}" ]; then
    . ./dynamic_resources.sh
    # Calculate a max heap size based some DYNAMIC_HEAP_FRACTION of the heap
    # available to a jvm using 100% of the CGroup-aware memory
    # up to some optional DYNAMIC_HEAP_MAX
    CALC_MAX_HEAP=$(get_heap_size "${DYNAMIC_HEAP_FRACTION}" "${DYNAMIC_HEAP_MAX}")
    if [ -n "$CALC_MAX_HEAP" ]; then
      export KAFKA_HEAP_OPTS="-Xms${CALC_MAX_HEAP} -Xmx${CALC_MAX_HEAP}"
    fi
fi

. ./set_kafka_gc_options.sh

# starting Kafka server with final configuration
exec /usr/bin/tini -w -e 143 -- "${KAFKA_HOME}/bin/kafka-server-start.sh" /tmp/strimzi.properties
