#!/usr/bin/env bash
set -e
set +x

# Clean-up /tmp directory from files which might have remained from previous container restart
# We ignore any errors which might be caused by files injected by different agents which we do not have the rights to delete
rm -rfv /tmp/* || true

STRIMZI_BROKER_ID=$(hostname | awk -F'-' '{print $NF}')
export STRIMZI_BROKER_ID
echo "STRIMZI_BROKER_ID=${STRIMZI_BROKER_ID}"

# Disable Kafka's GC logging (which logs to a file)...
export GC_LOG_ENABLED="false"

if [ -z "$KAFKA_LOG4J_OPTS" ]; then
  export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$KAFKA_HOME/custom-config/log4j.properties"
fi

. ./set_kafka_jmx_options.sh "${STRIMZI_JMX_ENABLED}" "${STRIMZI_JMX_USERNAME}" "${STRIMZI_JMX_PASSWORD}"

if [ -n "$STRIMZI_JAVA_SYSTEM_PROPERTIES" ]; then
    export KAFKA_OPTS="${KAFKA_OPTS} ${STRIMZI_JAVA_SYSTEM_PROPERTIES}"
fi

# Disable FIPS if needed
if [ "$FIPS_MODE" = "disabled" ]; then
    export KAFKA_OPTS="${KAFKA_OPTS} -Dcom.redhat.fips=false"
fi

# enabling Prometheus JMX exporter as Java agent
if [ "$KAFKA_METRICS_ENABLED" = "true" ]; then
  KAFKA_OPTS="${KAFKA_OPTS} -javaagent:$(ls "$KAFKA_HOME"/libs/jmx_prometheus_javaagent*.jar)=9404:$KAFKA_HOME/custom-config/metrics-config.json"
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

# Configure heap based on the available resources if needed
. ./dynamic_resources.sh

STRIMZI_KAFKA_METADATA_CONFIG_STATE=$(cat "$KAFKA_HOME"/custom-config/metadata.state)
echo "Kafka metadata config state [${STRIMZI_KAFKA_METADATA_CONFIG_STATE}]"
echo "$STRIMZI_KAFKA_METADATA_CONFIG_STATE" > /tmp/kafka/strimzi.kafka.metadata.config.state

source ./kraft_utils.sh
USE_KRAFT=$(useKRaft)
echo "Using KRaft [${USE_KRAFT}]"

# Prepare for Kraft
if [ "$USE_KRAFT" == "true" ]; then
  KRAFT_LOG_DIR=$(grep "log\.dirs=" /tmp/strimzi.properties | sed "s/log\.dirs=*//")

  if [ ! -f "$KRAFT_LOG_DIR/meta.properties" ]; then
    STRIMZI_CLUSTER_ID=$(cat "$KAFKA_HOME/custom-config/cluster.id")
    METADATA_VERSION=$(cat "$KAFKA_HOME/custom-config/metadata.version")
    echo "Formatting Kraft storage with cluster ID $STRIMZI_CLUSTER_ID and metadata version $METADATA_VERSION"
    mkdir -p "$KRAFT_LOG_DIR"
    # Using "=" to assign arguments for the Kafka storage tool to avoid issues if the generated
    # cluster ID starts with a "-". See https://issues.apache.org/jira/browse/KAFKA-15754
    ./bin/kafka-storage.sh format -t="$STRIMZI_CLUSTER_ID" -r="$METADATA_VERSION" -c=/tmp/strimzi.properties
  else
    echo "Kraft storage is already formatted"
  fi

  # remove quorum-state file so that we won't enter voter not match error after scaling up/down
  if [ -f "$KRAFT_LOG_DIR/__cluster_metadata-0/quorum-state" ]; then
    echo "Removing quorum-state file"
    rm -f "$KRAFT_LOG_DIR/__cluster_metadata-0/quorum-state"
  fi

  # when in KRaft mode, the Kafka ready and ZooKeeper connected file paths are empty because not needed to the agent
  KAFKA_READY=
  ZK_CONNECTED=
else
  KRAFT_LOG_DIR=$(grep "log\.dirs=" /tmp/strimzi.properties | sed "s/log\.dirs=*//")

  # when in ZooKeeper mode, the __cluster_metadata folder should not exist.
  # if it does, it means a KRaft migration rollback is ongoing and it has to be removed.
  if [ -d "$KRAFT_LOG_DIR/__cluster_metadata-0" ]; then
    echo "Removing __cluster_metadata folder"
    rm -rf "$KRAFT_LOG_DIR/__cluster_metadata-0"
  fi

  # when in ZooKeeper mode, the Kafka ready and ZooKeeper connected file paths are defined because used by the agent
  KAFKA_READY=/var/opt/kafka/kafka-ready
  ZK_CONNECTED=/var/opt/kafka/zk-connected
  rm -f $KAFKA_READY $ZK_CONNECTED 2> /dev/null
fi

KEY_STORE=/tmp/kafka/cluster.keystore.p12
TRUST_STORE=/tmp/kafka/cluster.truststore.p12
KAFKA_OPTS="${KAFKA_OPTS} -javaagent:$(ls "$KAFKA_HOME"/libs/kafka-agent*.jar)=$KAFKA_READY:$ZK_CONNECTED:$KEY_STORE:$CERTS_STORE_PASSWORD:$TRUST_STORE:$CERTS_STORE_PASSWORD"
export KAFKA_OPTS

# Configure Garbage Collection logging
. ./set_kafka_gc_options.sh

set -x

# starting Kafka server with final configuration
exec /usr/bin/tini -w -e 143 -- "${KAFKA_HOME}/bin/kafka-server-start.sh" /tmp/strimzi.properties
