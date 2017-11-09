#!/bin/bash

# volume for saving Kafka server logs
export KAFKA_VOLUME="/var/lib/kafka/"
# base name for Kafka server data dir and application logs
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

# starting Kafka server with final configuration
exec $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties \
--override broker.id=$KAFKA_BROKER_ID \
--override advertised.host.name=$(hostname -I) \
--override zookeeper.connect=$ZOOKEEPER_SERVICE_HOST:$ZOOKEEPER_SERVICE_PORT \
--override log.dirs=$KAFKA_LOG_DIRS \
--override default.replication.factor=${KAFKA_DEFAULT_REPLICATION_FACTOR:-1} \
--override offsets.topic.replication.factor=${KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR:-3} \
--override transaction.state.log.replication.factor=${KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR:-3}
