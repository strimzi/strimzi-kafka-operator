#!/usr/bin/env bash
set -e
set +x

# volume for saving Zookeeper server logs
export ZOOKEEPER_VOLUME="/var/lib/zookeeper/"
# base name for Zookeeper server data dir and application logs
export ZOOKEEPER_DATA_BASE_NAME="data"
export ZOOKEEPER_LOG_BASE_NAME="logs"

ZOOKEEPER_NODE_COUNT="$(cat /opt/kafka/custom-config/zookeeper.node-count)"
export ZOOKEEPER_NODE_COUNT
BASE_HOSTNAME=$(hostname | rev | cut -d "-" -f2- | rev)
export BASE_HOSTNAME
BASE_FQDN=$(hostname -f | cut -d "." -f2-4)
export BASE_FQDN

# Detect the server ID based on the hostname.
# Pods are numbered from 0 so we have to always increment by 1
ZOOKEEPER_ID=$(hostname | awk -F'-' '{print $NF+1}')
export ZOOKEEPER_ID
echo "Detected Zookeeper ID $ZOOKEEPER_ID"

# dir for saving application logs
export LOG_DIR=$ZOOKEEPER_VOLUME$ZOOKEEPER_LOG_BASE_NAME

# create data dir
export ZOOKEEPER_DATA_DIR=$ZOOKEEPER_VOLUME$ZOOKEEPER_DATA_BASE_NAME
mkdir -p $ZOOKEEPER_DATA_DIR

# Create myid file
echo "$ZOOKEEPER_ID" > $ZOOKEEPER_DATA_DIR/myid

# Generate temporary keystore password
CERTS_STORE_PASSWORD=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)
export CERTS_STORE_PASSWORD

mkdir -p /tmp/zookeeper

# Import certificates into keystore and truststore
./zookeeper_tls_prepare_certificates.sh

# Generate and print the config file
echo "Starting Zookeeper with configuration:"
./zookeeper_config_generator.sh | tee /tmp/zookeeper.properties | sed -e 's/password=.*/password=[hidden]/g'
echo ""

if [ -z "$KAFKA_LOG4J_OPTS" ]; then
  export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$KAFKA_HOME/custom-config/log4j.properties"
fi

# enabling Prometheus JMX exporter as Java agent
if [ "$ZOOKEEPER_METRICS_ENABLED" = "true" ]; then
  KAFKA_OPTS="$KAFKA_OPTS -javaagent:$(ls "$KAFKA_HOME"/libs/jmx_prometheus_javaagent*.jar)=9404:$KAFKA_HOME/custom-config/metrics-config.json"
  export KAFKA_OPTS
fi

. ./set_kafka_jmx_options.sh "${STRIMZI_JMX_ENABLED}" "${STRIMZI_JMX_USERNAME}" "${STRIMZI_JMX_PASSWORD}"

# Configure heap based on the available resources if needed
. ./dynamic_resources.sh

# Configure Garbage Collection logging
. ./set_kafka_gc_options.sh

if [ -n "$STRIMZI_JAVA_SYSTEM_PROPERTIES" ]; then
    export KAFKA_OPTS="${KAFKA_OPTS} ${STRIMZI_JAVA_SYSTEM_PROPERTIES}"
fi

# Disable FIPS if needed
if [ "$FIPS_MODE" = "disabled" ]; then
    export KAFKA_OPTS="${KAFKA_OPTS} -Dcom.redhat.fips=false"
fi

# We need to disable the native ZK authorisation (we secure ZK through the TLS-Sidecars) to allow use of the reconfiguration options.
KAFKA_OPTS="$KAFKA_OPTS -Dzookeeper.skipACL=yes"
# We set the electionPortBindRetry zo 0 to retry forever - the recommended option for Kubernetes
KAFKA_OPTS="$KAFKA_OPTS -Dzookeeper.electionPortBindRetry=0"
export KAFKA_OPTS

set -x

# starting Zookeeper with final configuration
exec /usr/bin/tini -w -e 143 -- "${KAFKA_HOME}/bin/zookeeper-server-start.sh" /tmp/zookeeper.properties
