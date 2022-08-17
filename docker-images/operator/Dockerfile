FROM strimzi/base:latest

LABEL org.opencontainers.image.source='https://github.com/strimzi/strimzi-kafka-operator'

# Add strimzi user with UID 1001
# The user is in the group 0 to have access to the mounted volumes and storage
RUN useradd -r -m -u 1001 -g 0 strimzi

ARG strimzi_version=1.0-SNAPSHOT
ENV STRIMZI_VERSION ${strimzi_version}
ENV STRIMZI_HOME=/opt/strimzi
RUN mkdir -p ${STRIMZI_HOME}/bin
WORKDIR ${STRIMZI_HOME}

COPY scripts/ tmp/bin/ bin/

COPY tmp/lib/ lib/

USER 1001
