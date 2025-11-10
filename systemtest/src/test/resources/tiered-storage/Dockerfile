ARG BASE_IMAGE

FROM ${BASE_IMAGE}

ARG AIVEN_PLUGIN_VERSION="1.1.1"
ARG TIERED_STORAGE_URL="https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/releases/download"

USER root:root
RUN mkdir -p /opt/kafka/plugins/tiered-storage

RUN curl -sL "$TIERED_STORAGE_URL/v$AIVEN_PLUGIN_VERSION/s3-$AIVEN_PLUGIN_VERSION.tgz" | tar -xz  --strip-components=1 -C "/opt/kafka/plugins/tiered-storage"
RUN curl -sL "$TIERED_STORAGE_URL/v$AIVEN_PLUGIN_VERSION/core-$AIVEN_PLUGIN_VERSION.tgz" | tar -xz  --strip-components=1 -C "/opt/kafka/plugins/tiered-storage"
RUN curl -sL "$TIERED_STORAGE_URL/v$AIVEN_PLUGIN_VERSION/filesystem-$AIVEN_PLUGIN_VERSION.tgz" | tar -xz  --strip-components=1 -C "/opt/kafka/plugins/tiered-storage"

RUN rm -rf /tmp/tiered-storage

USER 1001
