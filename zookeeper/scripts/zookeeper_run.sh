#!/bin/bash

# volume for saving Zookeeper server logs
export ZOOKEEPER_VOLUME="/tmp/zookeeper/"
# base name for Zookeeper server data dir and application logs
export ZOOKEEPER_DATA_BASE_NAME="data"
export ZOOKEEPER_LOG_BASE_NAME="logs"
# Disable JMX until we need it
export JMXDISABLE=true

# Until we run Zookeeper clusters we can hardcode the ID
export ZOOKEEPER_ID=1
echo "ZOOKEEPER_ID=$ZOOKEEPER_ID"

# create data dir
export ZOOKEEPER_DATA_DIR=$ZOOKEEPER_VOLUME$ZOOKEEPER_DATA_BASE_NAME$ZOOKEEPER_ID
echo "ZOOKEEPER_DATA_DIR=$ZOOKEEPER_DATA_DIR"

# dir for saving application logs
export LOG_DIR=$ZOOKEEPER_VOLUME$ZOOKEEPER_LOG_BASE_NAME$ZOOKEEPER_ID
echo "LOG_DIR=$LOG_DIR"

# environment variables substitution in the server configuration template file
envsubst < $ZOOKEEPER_HOME/config/zookeeper.properties.template > /tmp/zookeeper.properties

# starting Zookeeper with final configuration
exec $ZOOKEEPER_HOME/bin/zkServer.sh start-foreground /tmp/zookeeper.properties
