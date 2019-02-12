#!/usr/bin/env bash
set +x

export KAFKA_BROKER_ID=$(hostname | awk -F'-' '{print $NF}')
echo "KAFKA_BROKER_ID=$KAFKA_BROKER_ID"

# Kafka server data dirs
export KAFKA_LOG_DIR_PATH="kafka-log${KAFKA_BROKER_ID}"

for DIR in $(echo $KAFKA_LOG_DIRS | tr ',' ' ')  ; do
  export KAFKA_LOG_DIRS_WITH_PATH="${KAFKA_LOG_DIRS_WITH_PATH},${DIR}/${KAFKA_LOG_DIR_PATH}"
done

export KAFKA_LOG_DIRS_WITH_PATH="${KAFKA_LOG_DIRS_WITH_PATH:1}"

echo "KAFKA_LOG_DIRS=$KAFKA_LOG_DIRS_WITH_PATH"

# Disable Kafka's GC logging (which logs to a file)...
export GC_LOG_ENABLED="false"

if [ -z "$KAFKA_LOG4J_OPTS" ]; then
  export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$KAFKA_HOME/custom-config/log4j.properties"
fi

# enabling Prometheus JMX exporter as Java agent
if [ "$KAFKA_METRICS_ENABLED" = "true" ]; then
  export KAFKA_OPTS="-javaagent:/opt/prometheus/jmx_prometheus_javaagent.jar=9404:$KAFKA_HOME/custom-config/metrics-config.yml"
fi

# We don't need LOG_DIR because we write no log files, but setting it to a
# directory avoids trying to create it (and logging a permission denied error)
export LOG_DIR="$KAFKA_HOME"

# get broker rack if it's enabled
if [ -e $KAFKA_HOME/init/rack.id ]; then
  export KAFKA_RACK=$(cat $KAFKA_HOME/init/rack.id)
fi

# Generate temporary keystore password
export CERTS_STORE_PASSWORD=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)

mkdir -p /tmp/kafka

# Import certificates into keystore and truststore
./kafka_tls_prepare_certificates.sh

# Generate and print the config file
echo "Starting Kafka with configuration:"
./kafka_config_generator.sh | tee /tmp/strimzi.properties
echo ""

if [ -z "$KAFKA_HEAP_OPTS" -a -n "${DYNAMIC_HEAP_FRACTION}" ]; then
    . ./dynamic_resources.sh
    # Calculate a max heap size based some DYNAMIC_HEAP_FRACTION of the heap
    # available to a jvm using 100% of the CGroup-aware memory
    # up to some optional DYNAMIC_HEAP_MAX
    CALC_MAX_HEAP=$(get_heap_size ${DYNAMIC_HEAP_FRACTION} ${DYNAMIC_HEAP_MAX})
    if [ -n "$CALC_MAX_HEAP" ]; then
      export KAFKA_HEAP_OPTS="-Xms${CALC_MAX_HEAP} -Xmx${CALC_MAX_HEAP}"
    fi
fi

. ./set_kafka_gc_options.sh

# starting Kafka server with final configuration
exec $KAFKA_HOME/bin/kafka-server-start.sh /tmp/strimzi.properties
