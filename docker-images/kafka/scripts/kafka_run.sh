#!/bin/bash

# volume for saving Kafka server logs
export KAFKA_VOLUME="/var/lib/kafka/"
# base name for Kafka server data dir
export KAFKA_LOG_BASE_NAME="kafka-log"
export KAFKA_APP_LOGS_BASE_NAME="logs"

export KAFKA_BROKER_ID=$(hostname | awk -F'-' '{print $NF}')
echo "KAFKA_BROKER_ID=$KAFKA_BROKER_ID"

# create data dir
export KAFKA_LOG_DIRS=$KAFKA_VOLUME$KAFKA_LOG_BASE_NAME$KAFKA_BROKER_ID
echo "KAFKA_LOG_DIRS=$KAFKA_LOG_DIRS"

# Disable Kafka's GC logging (which logs to a file)...
export GC_LOG_ENABLED="false"
# ... but enable equivalent GC logging to stdout
export KAFKA_GC_LOG_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps"

if [ -z "$KAFKA_LOG_LEVEL" ]; then
  KAFKA_LOG_LEVEL="INFO"
fi
if [ -z "$KAFKA_LOG4J_OPTS" ]; then
  export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$KAFKA_HOME/config/log4j.properties -Dkafka.root.logger.level=$KAFKA_LOG_LEVEL,CONSOLE"
fi

# enabling Prometheus JMX exporter as Java agent
if [ "$KAFKA_METRICS_ENABLED" = "true" ]; then
  export KAFKA_OPTS="-javaagent:/opt/prometheus/jmx_prometheus_javaagent.jar=9404:/opt/prometheus/config/config.yml"
fi

# We don't need LOG_DIR because we write no log files, but setting it to a
# directory avoids trying to create it (and logging a permission denied error)
export LOG_DIR="$KAFKA_HOME"

# Write the config file
cat > /tmp/strimzi.properties <<EOF
broker.id=${KAFKA_BROKER_ID}
# Listeners
listeners=CLIENT://:9092,REPLICATION://:9091
advertised.listeners=REPLICATION:$(hostname -f)//:9091
listener.security.protocol.map=CLIENT:PLAINTEXT,REPLICATION:PLAINTEXT
inter.broker.listener.name=REPLICATION
# Zookeeper
zookeeper.connect=${KAFKA_ZOOKEEPER_CONNECT:-zookeeper:2181}
zookeeper.connection.timeout.ms=6000
# Logs
log.dirs=${KAFKA_LOG_DIRS}
num.partitions=1
num.recovery.threads.per.data.dir=1
default.replication.factor=${KAFKA_DEFAULT_REPLICATION_FACTOR:-1}
offsets.topic.replication.factor=${KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR:-3}
transaction.state.log.replication.factor=${KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR:-3}
transaction.state.log.min.isr=1
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
# Network
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
# Other
group.initial.rebalance.delay.ms=0
EOF

echo "Starting Kafka with configuration:"
cat /tmp/strimzi.properties
echo ""

# starting Kafka server with final configuration
exec $KAFKA_HOME/bin/kafka-server-start.sh /tmp/strimzi.properties
