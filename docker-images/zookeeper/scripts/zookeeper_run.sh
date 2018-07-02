#!/bin/bash

# volume for saving Zookeeper server logs
export ZOOKEEPER_VOLUME="/var/lib/zookeeper/"
# base name for Zookeeper server data dir and application logs
export ZOOKEEPER_DATA_BASE_NAME="data"
export ZOOKEEPER_LOG_BASE_NAME="logs"

export BASE_HOSTNAME=$(hostname | rev | cut -d "-" -f2- | rev)
export BASE_FQDN=$(hostname -f | cut -d "." -f2-)

# Detect the server ID based on the hostname.
# StatefulSets are numbered from 0 so we have to always increment by 1
export ZOOKEEPER_ID=$(hostname | awk -F'-' '{print $NF+1}')
echo "Detected Zookeeper ID $ZOOKEEPER_ID"

# dir for saving application logs
export LOG_DIR=$ZOOKEEPER_VOLUME$ZOOKEEPER_LOG_BASE_NAME

# create data dir
export ZOOKEEPER_DATA_DIR=$ZOOKEEPER_VOLUME$ZOOKEEPER_DATA_BASE_NAME
mkdir -p $ZOOKEEPER_DATA_DIR

# Create myid file
echo $ZOOKEEPER_ID > $ZOOKEEPER_DATA_DIR/myid

# Generate and print the config file
echo "Starting Zookeeper with configuration:"
./zookeeper_config_generator.sh | tee /tmp/zookeeper.properties
echo ""

if [ -z "$KAFKA_LOG4J_OPTS" ]; then
  export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$KAFKA_HOME/config/log4j.properties"
fi

# enabling Prometheus JMX exporter as Java agent
if [ "$ZOOKEEPER_METRICS_ENABLED" = "true" ]; then
  export KAFKA_OPTS="-javaagent:/opt/prometheus/jmx_prometheus_javaagent.jar=9404:$KAFKA_HOME/config/metrics-config.yml"
fi

if [ -z "$KAFKA_HEAP_OPTS" -a -n "${DYNAMIC_HEAP_FRACTION}" ]; then
    . ./dynamic_resources.sh
    # Calculate a max heap size based some DYNAMIC_HEAP_FRACTION of the heap
    # available to a jvm using 100% of the GCroup-aware memory
    # up to some optional DYNAMIC_HEAP_MAX
    CALC_MAX_HEAP=$(get_heap_size ${DYNAMIC_HEAP_FRACTION} ${DYNAMIC_HEAP_MAX})
    if [ -n "$CALC_MAX_HEAP" ]; then
      export KAFKA_HEAP_OPTS="-Xms${CALC_MAX_HEAP} -Xmx${CALC_MAX_HEAP}"
    fi
fi

# starting Zookeeper with final configuration
exec $KAFKA_HOME/bin/zookeeper-server-start.sh /tmp/zookeeper.properties
