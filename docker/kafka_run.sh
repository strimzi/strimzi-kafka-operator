#!/bin/bash

if [[ -z "$KAFKA_PORT" ]]; then
    export KAFKA_PORT=9092
fi

if [[ -z "$KAFKA_BROKER_ID" ]]; then
    # auto broker id generation
    export KAFKA_BROKER_ID=-1
fi

if [[ -z "$KAFKA_ADVERTISED_HOST" ]]; then
    export KAFKA_ADVERTISED_HOST=$(hostname -I)
fi

if [[ -z "$KAFKA_ADVERTISED_PORT" ]]; then
    export KAFKA_ADVERTISED_PORT=9092
fi

# volume for saving Kafka server logs
export KAFKA_VOLUME="/tmp/kafka/"
# base name for Kafka server logs dir
export KAFKA_LOGS_BASE_NAME="kafka-logs"

# the script finds the first available Kafka broker id as a new one
# or an existing one but with no lock on logs dirs
BASE=$(dirname $0)
export KAFKA_BROKER_ID=$($BASE/kafka_get_id.py $KAFKA_VOLUME $KAFKA_LOGS_BASE_NAME)

# environment variables substitution in the server configuration template file
envsubst < $KAFKA_HOME/config/server.properties.template > /tmp/server.properties

$BASE/kafka_pre_run.py /tmp/server.properties

# starting Kafka server with final configuration
exec $KAFKA_HOME/bin/kafka-server-start.sh /tmp/server.properties

