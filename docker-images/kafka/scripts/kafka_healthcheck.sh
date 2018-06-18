#!/bin/bash

if [ "$KAFKA_ENCRYPTION_ENABLED" = "true" ]; then
    ./bin/kafka-broker-api-versions.sh --bootstrap-server=localhost:9091 --command-config /tmp/healthcheck.properties
else
    ./bin/kafka-broker-api-versions.sh --bootstrap-server=localhost:9092
fi