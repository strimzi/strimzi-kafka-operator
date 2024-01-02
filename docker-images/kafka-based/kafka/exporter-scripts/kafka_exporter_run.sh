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

if [ -n "$KAFKA_EXPORTER_GROUP_EXCLUDE_REGEX" ]; then
    # shellcheck disable=SC2027
    groupExcludeRegex="--group.exclude=\""${KAFKA_EXPORTER_GROUP_EXCLUDE_REGEX}"\""
fi

if [ -n "$KAFKA_EXPORTER_TOPIC_EXCLUDE_REGEX" ]; then
    # shellcheck disable=SC2027
    topicExcludeRegex="--topic.exclude=\""${KAFKA_EXPORTER_TOPIC_EXCLUDE_REGEX}"\""
fi

if [ "$KAFKA_EXPORTER_ENABLE_SARAMA" = "true" ]; then
    saramaenable="--log.enable-sarama"
fi

if [ "$KAFKA_EXPORTER_OFFSET_SHOW_ALL" = "true" ]; then
    allgroups="--offset.show-all"
fi

if [ -n "$KAFKA_EXPORTER_LOGGING" ]; then
    loglevel="--verbosity=${KAFKA_EXPORTER_LOGGING}"
fi

# Combine all the certs in the cluster CA into one file
CA_CERTS=/tmp/cluster-ca.crt
for cert in /etc/kafka-exporter/cluster-ca-certs/*.crt; do
  sed -z '$ s/\n$//' "$cert" >> "$CA_CERTS"
  echo "" >> "$CA_CERTS"
done

# shellcheck disable=SC2027
version="--kafka.version=\""$KAFKA_EXPORTER_KAFKA_VERSION"\""

kafkaserver="--kafka.server="$KAFKA_EXPORTER_KAFKA_SERVER

listenaddress="--web.listen-address=:9404"

tls="--tls.enabled --tls.ca-file=$CA_CERTS --tls.cert-file=/etc/kafka-exporter/kafka-exporter-certs/kafka-exporter.crt  --tls.key-file=/etc/kafka-exporter/kafka-exporter-certs/kafka-exporter.key"

# starting Kafka Exporter with final configuration
cat <<EOT > /tmp/run.sh
$KAFKA_EXPORTER_HOME/kafka_exporter \
$groupregex \
$topicregex \
$groupExcludeRegex \
$topicExcludeRegex \
$tls \
$kafkaserver \
$saramaenable \
$listenaddress \
$allgroups \
$loglevel \
$version
EOT

chmod +x /tmp/run.sh

set -x

exec /usr/bin/tini -w -e 143 -- /tmp/run.sh
