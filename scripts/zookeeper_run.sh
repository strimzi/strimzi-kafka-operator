#!/bin/bash

# volume for saving Kafka server logs
export ZOOKEEPER_VOLUME="/tmp/zookeeper/"
# base name for Kafka server data dir and application logs
export ZOOKEEPER_DATA_BASE_NAME="data"
export ZOOKEEPER_LOG_BASE_NAME="logs"

echo "ZOOKEEPER_ID=$ZOOKEEPER_ID"

# create data dir
export ZOOKEEPER_DATA_DIR=$ZOOKEEPER_VOLUME$ZOOKEEPER_DATA_BASE_NAME$ZOOKEEPER_ID
mkdir $ZOOKEEPER_DATA_DIR
echo "ZOOKEEPER_DATA_DIR=$ZOOKEEPER_DATA_DIR"

# dir for saving application logs
export LOG_DIR=$ZOOKEEPER_VOLUME$ZOOKEEPER_LOG_BASE_NAME$ZOOKEEPER_ID
echo "LOG_DIR=$LOG_DIR"

# environment variables substitution in the server configuration template file
envsubst < $KAFKA_HOME/config/zookeeper.properties.template > /tmp/zookeeper.properties

# only if zookeeper id is defined, clustered (multi-server) setup
# there is the need for building the server lists in the config file
if [ -n "$ZOOKEEPER_ID" ]; then
  # writing Zookeeper server id into "myid" file
  echo $ZOOKEEPER_ID > "$ZOOKEEPER_DATA_DIR/myid"
  BASE=$(dirname $0)
  $BASE/zookeeper_pre_run.py /tmp/zookeeper.properties
fi

# starting Zookeeper with final configuration
exec $KAFKA_HOME/bin/zookeeper-server-start.sh /tmp/zookeeper.properties
