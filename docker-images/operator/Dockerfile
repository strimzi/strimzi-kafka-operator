FROM strimzi/base:latest

LABEL org.opencontainers.image.source='https://github.com/strimzi/strimzi-kafka-operator'

# Add strimzi user with UID 1001
# The user is in the group 0 to have access to the mounted volumes and storage
RUN echo "strimzi:x:1001:0::/home/strimzi:/bin/false" >> /etc/passwd

ARG strimzi_version=1.0-SNAPSHOT

LABEL name='operator' \
    summary='Operator image of the Strimzi Kafka Operator project.' \
    description='Operator image used by the cluster and entity operator components of the Strimzi Kafka Operator.'

ENV STRIMZI_VERSION=${strimzi_version}
ENV STRIMZI_HOME=/opt/strimzi
RUN mkdir -p ${STRIMZI_HOME}/bin
WORKDIR ${STRIMZI_HOME}

COPY scripts/ tmp/bin/ bin/

COPY tmp/lib/ lib/

USER 1001
