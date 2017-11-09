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

# Disable Kafka's GC logging (which logs to a file)...
export GC_LOG_ENABLED="false"
# ... but enable equivalent GC logging to stdout
export KAFKA_GC_LOG_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps"

# starting Kafka server with final configuration
exec $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties \
--override broker.id=$KAFKA_BROKER_ID \
--override advertised.host.name=$(hostname -I) \
--override zookeeper.connect=${KAFKA_ZOOKEEPER_CONNECT:-zookeeper:2181}  \
--override log.dirs=$KAFKA_LOG_DIRS \
--override default.replication.factor=${KAFKA_DEFAULT_REPLICATION_FACTOR:-1} \
--override offsets.topic.replication.factor=${KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR:-3} \
--override transaction.state.log.replication.factor=${KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR:-3}
