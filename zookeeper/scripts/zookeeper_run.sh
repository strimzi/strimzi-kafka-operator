#!/bin/bash

# volume for saving Kafka server logs
export ZOOKEEPER_VOLUME="/tmp/zookeeper/"
# base name for Kafka server data dir and application logs
export ZOOKEEPER_DATA_BASE_NAME="data"
export ZOOKEEPER_LOG_BASE_NAME="logs"

# TODO: Zookeeper cluster support. 
# Taking ID from hostname makes no sense if we don't generate config properly
#export ZOOKEEPER_ID=$(hostname | awk -F'-' '{print $2}')
export ZOOKEEPER_ID=1
echo "ZOOKEEPER_ID=$ZOOKEEPER_ID"

# create data dir
export ZOOKEEPER_DATA_DIR=$ZOOKEEPER_VOLUME$ZOOKEEPER_DATA_BASE_NAME$ZOOKEEPER_ID
echo "ZOOKEEPER_DATA_DIR=$ZOOKEEPER_DATA_DIR"

# dir for saving application logs
export LOG_DIR=$ZOOKEEPER_VOLUME$ZOOKEEPER_LOG_BASE_NAME$ZOOKEEPER_ID
echo "LOG_DIR=$LOG_DIR"

# environment variables substitution in the server configuration template file
envsubst < $KAFKA_HOME/config/zookeeper.properties.template > /tmp/zookeeper.properties

# starting Zookeeper with final configuration
exec $KAFKA_HOME/bin/zookeeper-server-start.sh /tmp/zookeeper.properties
