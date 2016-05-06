#!/bin/sh

envsubst < /opt/kafka_$SCALA_VERSION-$KAFKA_VERSION/config/server.properties.template > /tmp/server.properties
exec /opt/kafka_$SCALA_VERSION-$KAFKA_VERSION/bin/kafka-server-start.sh /tmp/server.properties