#!/bin/bash

if [[ -z "$KAFKA_BROKER_ID" ]]; then
    # auto broker id generation
    export KAFKA_BROKER_ID=-1
fi

if [[ -z "$KAFKA_ADVERTISED_HOST" ]]; then
    export KAFKA_ADVERTISED_HOST=$(hostname -I)
fi
envsubst < /opt/kafka_$SCALA_VERSION-$KAFKA_VERSION/config/server.properties.template > /tmp/server.properties
exec /opt/kafka_$SCALA_VERSION-$KAFKA_VERSION/bin/kafka-server-start.sh /tmp/server.properties
