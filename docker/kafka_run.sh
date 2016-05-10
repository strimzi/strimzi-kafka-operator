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

# environment variables substitution in the server configuration template file
envsubst < $KAFKA_HOME/config/server.properties.template > /tmp/server.properties
# starting Kafka server with final configuration
exec $KAFKA_HOME/bin/kafka-server-start.sh /tmp/server.properties
