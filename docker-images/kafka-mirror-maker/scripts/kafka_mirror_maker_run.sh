#!/bin/bash

if [ -n "$KAFKA_MIRRORMAKER_TRUSTED_CERTS_CONSUMER" ] || [ -n "$KAFKA_MIRRORMAKER_TRUSTED_CERTS_PRODUCER" ]; then
    # Generate temporary keystore password
    export CERTS_STORE_PASSWORD=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)

    mkdir -p /tmp/kafka

    # Import certificates into keystore and truststore
    kafka_mirror_maker_tls_prepare_certificates.sh
fi

# Generate and print the consumer config file
echo "Kafka Mirror Maker consumer configuration:"
./kafka_mirror_maker_consumer_config_generator.sh | tee /tmp/strimzi-consumer.properties
echo ""

# Generate and print the producer config file
echo "Kafka Mirror Maker producer configuration:"
./kafka_mirror_maker_producer_config_generator.sh | tee /tmp/strimzi-producer.properties
echo ""


if [ -n "$KAFKA_MIRRORMAKER_WHITELIST" ]; then
    whitelist="--whitelist ${KAFKA_MIRRORMAKER_WHITELIST}"
fi

if [ -n "$KAFKA_MIRRORMAKER_NUMSTREAMS_CONSUMER" ]; then
    numstreams="--num.streams ${KAFKA_MIRRORMAKER_NUMSTREAMS_CONSUMER}"
fi

# starting Kafka Mirror Maker with final configuration
exec $KAFKA_HOME/bin/kafka-mirror-maker.sh \
--consumer.config /tmp/strimzi-consumer.properties \
--producer.config /tmp/strimzi-producer.properties \
$whitelist \
$numstreams
