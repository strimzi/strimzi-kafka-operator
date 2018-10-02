#!/bin/bash

if [ -n "$KAFKA_MIRRORMAKER_TRUSTED_CERTS_CONSUMER" ] || [ -n "$KAFKA_MIRRORMAKER_TRUSTED_CERTS_PRODUCER" ]; then
    # Generate temporary keystore password
    export CERTS_STORE_PASSWORD=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)

    mkdir -p /tmp/kafka

    # Import certificates into keystore and truststore
    # $1 = trusted certs, $2 = TLS auth cert, $3 = TLS auth key, $4 = truststore path, $5 = keystore path, $6 = certs and key path
    ./kafka_mirror_maker_tls_prepare_certificates.sh \
        "$KAFKA_MIRRORMAKER_TRUSTED_CERTS_CONSUMER" \
        "$KAFKA_MIRRORMAKER_TLS_AUTH_CERT_CONSUMER" \
        "$KAFKA_MIRRORMAKER_TLS_AUTH_KEY_CONSUMER" \
        "/tmp/kafka/consumer.truststore.p12" \
        "/tmp/kafka/consumer.keystore.p12" \
        "/opt/kafka/consumer-certs"

    ./kafka_mirror_maker_tls_prepare_certificates.sh \
        "$KAFKA_MIRRORMAKER_TRUSTED_CERTS_PRODUCER" \
        "$KAFKA_MIRRORMAKER_TLS_AUTH_CERT_PRODUCER" \
        "$KAFKA_MIRRORMAKER_TLS_AUTH_KEY_PRODUCER" \
        "/tmp/kafka/producer.truststore.p12" \
        "/tmp/kafka/producer.keystore.p12" \
        "/opt/kafka/producer-certs"
fi

# Generate and print the consumer config file
echo "Kafka Mirror Maker consumer configuration:"
./kafka_mirror_maker_consumer_config_generator.sh | tee /tmp/strimzi-consumer.properties
echo ""

# Generate and print the producer config file
echo "Kafka Mirror Maker producer configuration:"
./kafka_mirror_maker_producer_config_generator.sh | tee /tmp/strimzi-producer.properties
echo ""

# Disable Kafka's GC logging (which logs to a file)...
export GC_LOG_ENABLED="false"
# ... but enable equivalent GC logging to stdout
export KAFKA_GC_LOG_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps"

if [ -z "$KAFKA_LOG4J_OPTS" ]; then
export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$KAFKA_HOME/custom-config/log4j.properties"
fi

# enabling Prometheus JMX exporter as Java agent
if [ "$KAFKA_MIRRORMAKER_METRICS_ENABLED" = "true" ]; then
  export KAFKA_OPTS="-javaagent:/opt/prometheus/jmx_prometheus_javaagent.jar=9404:$KAFKA_HOME/custom-config/metrics-config.yml"
fi

if [ -z "$KAFKA_HEAP_OPTS" -a -n "${DYNAMIC_HEAP_FRACTION}" ]; then
    . ./dynamic_resources.sh
    # Calculate a max heap size based some DYNAMIC_HEAP_FRACTION of the heap
    # available to a jvm using 100% of the GCroup-aware memory
    # up to some optional DYNAMIC_HEAP_MAX
    CALC_MAX_HEAP=$(get_heap_size ${DYNAMIC_HEAP_FRACTION} ${DYNAMIC_HEAP_MAX})
    if [ -n "$CALC_MAX_HEAP" ]; then
      export KAFKA_HEAP_OPTS="-Xms${CALC_MAX_HEAP} -Xmx${CALC_MAX_HEAP}"
    fi
fi

if [ -n "$KAFKA_MIRRORMAKER_WHITELIST" ]; then
    whitelist="--whitelist \""${KAFKA_MIRRORMAKER_WHITELIST}"\""
fi

if [ -n "$KAFKA_MIRRORMAKER_NUMSTREAMS" ]; then
    numstreams="--num.streams ${KAFKA_MIRRORMAKER_NUMSTREAMS}"
fi

# starting Kafka Mirror Maker with final configuration
exec $KAFKA_HOME/bin/kafka-mirror-maker.sh \
--consumer.config /tmp/strimzi-consumer.properties \
--producer.config /tmp/strimzi-producer.properties \
$whitelist \
$numstreams
