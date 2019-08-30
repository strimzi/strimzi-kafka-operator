#!/usr/bin/env bash
set +x

# We don't need LOG_DIR because we write no log files, but setting it to a
# directory avoids trying to create it (and logging a permission denied error)
export LOG_DIR="$KAFKA_EXPORTER_HOME"

if [ -n "$KAFKA_EXPORTER_GROUP_REGEX" ]; then
    groupregex="--group.filter=\""${KAFKA_EXPORTER_GROUP_REGEX}"\""
fi

if [ -n "$KAFKA_EXPORTER_TOPIC_REGEX" ]; then
    topicregex="--topic.filter=\""${KAFKA_EXPORTER_TOPIC_REGEX}"\""
fi

if [ "$KAFKA_EXPORTER_ENABLE_SARAMA" = "true" ]; then
    saramaenable="--log.enable-sarama"
fi


if [ -n "$KAFKA_EXPORTER_LOGGING" ]; then
    loglevel="--log.level=${KAFKA_EXPORTER_LOGGING}"
fi

version="--kafka.version="$KAFKA_EXPORTER_KAFKA_VERSION


kafkaserver="--kafka.server="$KAFKA_EXPORTER_KAFKA_SERVER

listenaddress="--web.listen-address=:9404"

# starting Kafka Exporter with final configuration
exec $KAFKA_EXPORTER_HOME/kafka_exporter \
$groupregex \
$topicregex \
$kafkaserver \
$saramaenable \
$listenaddress \
$loglevel \
$version
