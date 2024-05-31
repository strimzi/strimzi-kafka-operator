ARG BASE_IMAGE

FROM ${BASE_IMAGE}

ARG AIVEN_PLUGIN_VERSION="2024-04-02-1712056402"
ARG TIERED_STORAGE_URL="https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/releases/download/"

USER root:root
RUN mkdir -p /opt/kafka/plugins/tiered-storage

RUN curl -sL "$TIERED_STORAGE_URL/$AIVEN_PLUGIN_VERSION/s3-0.0.1-SNAPSHOT.tgz" | tar -xz  --strip-components=1 -C "/opt/kafka/plugins/tiered-storage"
RUN curl -sL "$TIERED_STORAGE_URL/$AIVEN_PLUGIN_VERSION/core-0.0.1-SNAPSHOT.tgz" | tar -xz  --strip-components=1 -C "/opt/kafka/plugins/tiered-storage"

RUN rm -rf /tmp/tiered-storage

USER 1001
