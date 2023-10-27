ARG BASE_IMAGE=quay.io/strimzi/kafka:latest-kafka
ARG KAFKA_VERSION

FROM ${BASE_IMAGE}-${KAFKA_VERSION}

USER root:root
RUN mkdir -p /opt/kafka/plugins/file-sink

ADD --chmod=444 https://repo1.maven.org/maven2/org/apache/kafka/connect-file/${KAFKA_VERSION}/connect-file-${KAFKA_VERSION}.jar /opt/kafka/plugins/file-sink/

USER 1001