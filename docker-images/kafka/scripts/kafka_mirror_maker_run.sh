#!/usr/bin/env bash
set +x

# Generate temporary keystore password
export CERTS_STORE_PASSWORD=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)

# Create dir where keystores and truststores will be stored
mkdir -p /tmp/kafka

# Import certificates into keystore and truststore
# $1 = trusted certs, $2 = TLS auth cert, $3 = TLS auth key, $4 = truststore path, $5 = keystore path, $6 = certs and key path
./kafka_mirror_maker_tls_prepare_certificates.sh \
    "$KAFKA_MIRRORMAKER_TRUSTED_CERTS_CONSUMER" \
    "$KAFKA_MIRRORMAKER_TLS_AUTH_CERT_CONSUMER" \
    "$KAFKA_MIRRORMAKER_TLS_AUTH_KEY_CONSUMER" \
    "/tmp/kafka/consumer.truststore.p12" \
    "/tmp/kafka/consumer.keystore.p12" \
    "/opt/kafka/consumer-certs" \
    "/opt/kafka/consumer-oauth-certs" \
    "/tmp/kafka/consumer-oauth.keystore.p12"

./kafka_mirror_maker_tls_prepare_certificates.sh \
    "$KAFKA_MIRRORMAKER_TRUSTED_CERTS_PRODUCER" \
    "$KAFKA_MIRRORMAKER_TLS_AUTH_CERT_PRODUCER" \
    "$KAFKA_MIRRORMAKER_TLS_AUTH_KEY_PRODUCER" \
    "/tmp/kafka/producer.truststore.p12" \
    "/tmp/kafka/producer.keystore.p12" \
    "/opt/kafka/producer-certs" \
    "/opt/kafka/producer-oauth-certs" \
    "/tmp/kafka/producer-oauth.keystore.p12"

# Generate and print the consumer config file
echo "Kafka Mirror Maker consumer configuration:"
./kafka_mirror_maker_consumer_config_generator.sh | tee /tmp/strimzi-consumer.properties | sed -e 's/sasl.jaas.config=.*/sasl.jaas.config=[hidden]/g' -e 's/password=.*/password=[hidden]/g'
echo ""

# Generate and print the producer config file
echo "Kafka Mirror Maker producer configuration:"
./kafka_mirror_maker_producer_config_generator.sh | tee /tmp/strimzi-producer.properties | sed -e 's/sasl.jaas.config=.*/sasl.jaas.config=[hidden]/g' -e 's/password=.*/password=[hidden]/g'
echo ""

# Disable Kafka's GC logging (which logs to a file)...
export GC_LOG_ENABLED="false"

if [ -z "$KAFKA_LOG4J_OPTS" ]; then
export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$KAFKA_HOME/custom-config/log4j.properties"
fi

# We don't need LOG_DIR because we write no log files, but setting it to a
# directory avoids trying to create it (and logging a permission denied error)
export LOG_DIR="$KAFKA_HOME"

# Enabling the Mirror Maker agent which monitors readiness / liveness
rm /tmp/mirror-maker-ready /tmp/mirror-maker-alive 2> /dev/null
export KAFKA_OPTS="$KAFKA_OPTS -javaagent:$(ls $KAFKA_HOME/libs/mirror-maker-agent*.jar)=/tmp/mirror-maker-ready:/tmp/mirror-maker-alive:${STRIMZI_READINESS_PERIOD:-10}:${STRIMZI_LIVENESS_PERIOD:-10}"

# enabling Prometheus JMX exporter as Java agent
if [ "$KAFKA_MIRRORMAKER_METRICS_ENABLED" = "true" ]; then
  export KAFKA_OPTS="$KAFKA_OPTS -javaagent:$(ls $KAFKA_HOME/libs/jmx_prometheus_javaagent*.jar)=9404:$KAFKA_HOME/custom-config/metrics-config.yml"
fi

# enabling Tracing agent (initializes Jaeger tracing) as Java agent
if [ "$STRIMZI_TRACING" = "jaeger" ]; then
  export KAFKA_OPTS="$KAFKA_OPTS -javaagent:$(ls $KAFKA_HOME/libs/tracing-agent*.jar)=jaeger"
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

if [ -n "$KAFKA_MIRRORMAKER_OFFSET_COMMIT_INTERVAL" ]; then
    offset_commit_interval="--offset.commit.interval.ms $KAFKA_MIRRORMAKER_OFFSET_COMMIT_INTERVAL"
fi

if [ -n "$KAFKA_MIRRORMAKER_ABORT_ON_SEND_FAILURE" ]; then
    abort_on_send_failure="--abort.on.send.failure $KAFKA_MIRRORMAKER_ABORT_ON_SEND_FAILURE"
fi

if [ -n "$KAFKA_MIRRORMAKER_MESSAGE_HANDLER" ]; then
    message_handler="--message.handler $KAFKA_MIRRORMAKER_MESSAGE_HANDLER"
fi

if [ -n "$KAFKA_MIRRORMAKER_MESSAGE_HANDLER_ARGS" ]; then
    message_handler_args="--message.handler.args \""${$KAFKA_MIRRORMAKER_MESSAGE_HANDLER_ARGS}"\""
fi

if [ -n "$STRIMZI_JAVA_SYSTEM_PROPERTIES" ]; then
    export KAFKA_OPTS="${KAFKA_OPTS} ${STRIMZI_JAVA_SYSTEM_PROPERTIES}"
fi

. ./set_kafka_gc_options.sh

# starting Kafka Mirror Maker with final configuration
exec /usr/bin/tini -w -e 143 -- $KAFKA_HOME/bin/kafka-mirror-maker.sh \
--consumer.config /tmp/strimzi-consumer.properties \
--producer.config /tmp/strimzi-producer.properties \
$whitelist \
$numstreams \
$offset_commit_interval \
$abort_on_send_failure \
$message_handler \
$message_handler_args
