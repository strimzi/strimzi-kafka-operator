ARG KAFKA_VERSION
ARG BASE_IMAGE=quay.io/strimzi/kafka:latest-kafka-${KAFKA_VERSION}

FROM ${BASE_IMAGE}

# to make KAFKA_VERSION arg available again after FROM, we need to specify it once more (different build stage)
ARG KAFKA_VERSION

USER root:root
RUN mkdir -p /opt/kafka/plugins/file-sink

ADD --chmod=444 https://repo1.maven.org/maven2/org/apache/kafka/connect-file/${KAFKA_VERSION}/connect-file-${KAFKA_VERSION}.jar /opt/kafka/plugins/file-sink/

USER 1001