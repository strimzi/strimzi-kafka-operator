#!/usr/bin/env bash
set -e
set +x

# Generate temporary keystore password
CERTS_STORE_PASSWORD=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)
export CERTS_STORE_PASSWORD

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
rm -f /tmp/mirror-maker-ready /tmp/mirror-maker-alive 2> /dev/null
KAFKA_OPTS="$KAFKA_OPTS -javaagent:$(ls "$KAFKA_HOME"/libs/mirror-maker-agent*.jar)=/tmp/mirror-maker-ready:/tmp/mirror-maker-alive:${STRIMZI_READINESS_PERIOD:-10}:${STRIMZI_LIVENESS_PERIOD:-10}"
export KAFKA_OPTS

# enabling Prometheus JMX exporter as Java agent
if [ "$KAFKA_MIRRORMAKER_METRICS_ENABLED" = "true" ]; then
  KAFKA_OPTS="$KAFKA_OPTS -javaagent:$(ls "$KAFKA_HOME"/libs/jmx_prometheus_javaagent*.jar)=9404:$KAFKA_HOME/custom-config/metrics-config.json"
  export KAFKA_OPTS
fi

# Disable FIPS if needed
if [ "$FIPS_MODE" = "disabled" ]; then
    export KAFKA_OPTS="${KAFKA_OPTS} -Dcom.redhat.fips=false"
fi

# enabling Tracing agent (initializes tracing) as Java agent
if [ "$STRIMZI_TRACING" = "jaeger" ] || [ "$STRIMZI_TRACING" = "opentelemetry" ]; then
    KAFKA_OPTS="$KAFKA_OPTS -javaagent:$(ls "$KAFKA_HOME"/libs/tracing-agent*.jar)=$STRIMZI_TRACING"
    export KAFKA_OPTS
    if [ "$STRIMZI_TRACING" = "opentelemetry" ] && [ -z "$OTEL_TRACES_EXPORTER" ]; then
      # auto-set OTLP exporter
      export OTEL_TRACES_EXPORTER="otlp"
    fi
fi

if [ -n "$KAFKA_MIRRORMAKER_INCLUDE" ]; then
    # shellcheck disable=SC2089
    include="--whitelist \"${KAFKA_MIRRORMAKER_INCLUDE}\""
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
    # shellcheck disable=SC2089
    message_handler_args="--message.handler.args \"${KAFKA_MIRRORMAKER_MESSAGE_HANDLER_ARGS}\""
fi

if [ -n "$STRIMZI_JAVA_SYSTEM_PROPERTIES" ]; then
    export KAFKA_OPTS="${KAFKA_OPTS} ${STRIMZI_JAVA_SYSTEM_PROPERTIES}"
fi

# Configure heap based on the available resources if needed
. ./dynamic_resources.sh

# Configure Garbage Collection logging
. ./set_kafka_gc_options.sh

set -x

# starting Kafka Mirror Maker with final configuration
# shellcheck disable=SC2086,SC2090
exec /usr/bin/tini -w -e 143 -- "$KAFKA_HOME"/bin/kafka-mirror-maker.sh \
--consumer.config /tmp/strimzi-consumer.properties \
--producer.config /tmp/strimzi-producer.properties \
$include \
$numstreams \
$offset_commit_interval \
$abort_on_send_failure \
$message_handler \
$message_handler_args
