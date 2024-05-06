ARG KAFKA_VERSION
ARG BASE_IMAGE=quay.io/strimzi/kafka:latest-kafka-${KAFKA_VERSION}

FROM ${BASE_IMAGE}

# to make KAFKA_VERSION arg available again after FROM, we need to specify it once more (different build stage)
ARG KAFKA_VERSION

USER root:root
RUN mkdir -p /opt/kafka/plugins/file-sink

RUN ls /opt/kafka/libs/* -d | grep .*connect-file-${KAFKA_VERSION}.*.jar -o | xargs cp -t /opt/kafka/plugins/file-sink

USER 1001