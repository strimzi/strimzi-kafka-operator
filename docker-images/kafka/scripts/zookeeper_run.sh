#!/usr/bin/env bash

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
echo "$ZOOKEEPER_ID" > $ZOOKEEPER_DATA_DIR/myid

# Generate and print the config file
echo "Starting Zookeeper with configuration:"
./zookeeper_config_generator.sh | tee /tmp/zookeeper.properties
echo ""

if [ -z "$KAFKA_LOG4J_OPTS" ]; then
  export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$KAFKA_HOME/custom-config/log4j.properties"
fi

# enabling Prometheus JMX exporter as Java agent
if [ "$ZOOKEEPER_METRICS_ENABLED" = "true" ]; then
  export KAFKA_OPTS="$KAFKA_OPTS -javaagent:$(ls $KAFKA_HOME/libs/jmx_prometheus_javaagent*.jar)=9404:$KAFKA_HOME/custom-config/metrics-config.yml"
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

. ./set_kafka_gc_options.sh

############ ZK Upgrade Start ############
# ZK 3.4.x -> 3.5.x upgrade adds snapshot checks that can cause ZK pods with persistent storage to fail on startup
# The section below disables the snapshot checks until the servers have created a snapshot.

# If the CO has indicated this is an upgrade from a 3.4.x server then place an indicator file in the data directory
# so this persists over multiple restarts
flag_file=${ZOOKEEPER_DATA_DIR}/zk_upgrade_from_3_4
if [[ "$ZOOKEEPER_SNAPSHOT_CHECK_ENABLED" = "false" ]]; then
    echo "Detected 3.4.x server, creating flag file for snapshot checks after future upgrade to 3.5.6"
    touch ${flag_file}
fi

# If this server is attached to a disk that was once used by a 3.4.x server then we may need to disable snapshot checks
if [[ -f ${flag_file} ]]; then
    # If the version-2 folder does not exist then this is first start up of a 3.4.x server therefore this is not an upgrade so we can skip
    if [[ -d ${ZOOKEEPER_DATA_DIR}/version-2 ]]; then
        # If the 3.4 flag is there and the data directory is present this might be an upgrade so check for snapshots
        if [[ $(find ${ZOOKEEPER_DATA_DIR}/version-2 -maxdepth 1 -name "snapshot.*") ]]; then
            # If there are snapshot files then we will have a stable server after the upgrade and can remove the flag file
            rm ${flag_file}
            echo "Snapshots now present in data directory, enabling snapshot integrity checks"
        else
            # If there are no snapshot files and we started from a 3.4.x server then disable the snapshot checks for this boot
            KAFKA_OPTS="$KAFKA_OPTS -Dzookeeper.snapshot.trust.empty=true"
            export KAFKA_OPTS
            echo "No snapshots present in data directory, disabling snapshot integrity checks for this start up"
        fi
    fi
fi

# This section should be removed once Strimzi no longer supports Kafka brokers using ZK 3.4.x.
############ ZK Upgrade End ############

# starting Zookeeper with final configuration
exec /usr/bin/tini -w -e 143 -- sh -c "${KAFKA_HOME}/bin/zookeeper-server-start.sh /tmp/zookeeper.properties"
