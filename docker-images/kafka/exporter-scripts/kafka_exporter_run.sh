#!/usr/bin/env bash
set -e
set +x

# We don't need LOG_DIR because we write no log files, but setting it to a
# directory avoids trying to create it (and logging a permission denied error)
export LOG_DIR="$KAFKA_EXPORTER_HOME"

if [ -n "$KAFKA_EXPORTER_GROUP_REGEX" ]; then
    # shellcheck disable=SC2027
    groupregex="--group.filter=\""${KAFKA_EXPORTER_GROUP_REGEX}"\""
fi

if [ -n "$KAFKA_EXPORTER_TOPIC_REGEX" ]; then
    # shellcheck disable=SC2027
    topicregex="--topic.filter=\""${KAFKA_EXPORTER_TOPIC_REGEX}"\""
fi

if [ "$KAFKA_EXPORTER_ENABLE_SARAMA" = "true" ]; then
    saramaenable="--log.enable-sarama"
fi

if [ -n "$KAFKA_EXPORTER_LOGGING" ]; then
    loglevel="--log.level=${KAFKA_EXPORTER_LOGGING}"
fi

# shellcheck disable=SC2027
version="--kafka.version=\""$KAFKA_EXPORTER_KAFKA_VERSION"\""

kafkaserver="--kafka.server="$KAFKA_EXPORTER_KAFKA_SERVER

listenaddress="--web.listen-address=:9404"

tls="--tls.enabled --tls.ca-file=/etc/kafka-exporter/cluster-ca-certs/ca.crt --tls.cert-file=/etc/kafka-exporter/kafka-exporter-certs/kafka-exporter.crt  --tls.key-file=/etc/kafka-exporter/kafka-exporter-certs/kafka-exporter.key"

sasl="--no-sasl.handshake"

# starting Kafka Exporter with final configuration
cat <<EOT > /tmp/run.sh
$KAFKA_EXPORTER_HOME/kafka_exporter \
$groupregex \
$topicregex \
$tls \
$kafkaserver \
$saramaenable \
$listenaddress \
$loglevel \
$sasl \
$version
EOT

chmod +x /tmp/run.sh

exec /usr/bin/tini -w -e 143 -- /tmp/run.sh
