FROM centos:7

ARG JAVA_VERSION
ARG KAFKA_SHA512
ARG KAFKA_VERSION
ARG THIRD_PARTY_LIBS
ARG strimzi_version

RUN yum -y update \
    && yum -y install java-${JAVA_VERSION}-openjdk-headless gettext nmap-ncat openssl \
    && yum clean all -y

# set Kafka home folder
ENV KAFKA_HOME=/opt/kafka

ENV KAFKA_EXPORTER_HOME=/opt/kafka-exporter

# Add kafka user with UID 1001
# The user is in the group 0 to have access to the mounted volumes and storage
RUN useradd -r -m -u 1001 -g 0 kafka

# Set Scala and Kafka version
ENV KAFKA_VERSION=${KAFKA_VERSION}
ENV STRIMZI_VERSION=${strimzi_version}
ENV SCALA_VERSION=2.12

ENV KAFKA_EXPORTER_VERSION=1.2.0
ENV KAFKA_EXPORTER_CHECKSUM="7afa40365ddf0cb0a88457684bd64d565e250c7e5a4536ba7f9d37d02d2808c3b07766f94e0b1338beb296573ade29db630948c931be44bde416c0410b5d783b  kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz"

# Set Kafka (SHA512) and Prometheus JMX exporter (SHA1) checksums
ENV KAFKA_CHECKSUM="${KAFKA_SHA512}  kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"

# Downloading/extracting Apache Kafka
RUN curl -O https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz \
    && echo $KAFKA_CHECKSUM > kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz.sha512 \
    && sha512sum --check kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz.sha512 \
    && mkdir $KAFKA_HOME \
    && tar xvfz kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C $KAFKA_HOME --strip-components=1 \
    && rm -f kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz*

# Downloading/extracting Kafka Exporter
RUN curl -LO https://github.com/danielqsj/kafka_exporter/releases/download/v${KAFKA_EXPORTER_VERSION}/kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz \
    && echo $KAFKA_EXPORTER_CHECKSUM > kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz.sha512 \
    && sha512sum --check kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz.sha512 \
    && mkdir $KAFKA_EXPORTER_HOME \
    && tar xvfz kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz -C $KAFKA_EXPORTER_HOME --strip-components=1 \
    && rm -f kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz*

COPY ./tmp/kafka-agent.jar ${KAFKA_HOME}/libs/
COPY ./tmp/mirror-maker-agent.jar ${KAFKA_HOME}/libs/
COPY ./tmp/tracing-agent.jar ${KAFKA_HOME}/libs/

COPY kafka-thirdparty-libs/${THIRD_PARTY_LIBS}/target/dependency/ ${KAFKA_HOME}/libs/

RUN yum -y install stunnel net-tools bind-utils && yum clean all -y

# set Stunnel home folder
ENV STUNNEL_HOME=/opt/stunnel

RUN mkdir $STUNNEL_HOME && mkdir -p -m g+rw /usr/local/var/run/

# copy scripts for Stunnel
COPY ./stunnel-scripts/ $STUNNEL_HOME

# copy scripts for starting Kafka
COPY ./scripts/ $KAFKA_HOME

# Set S2I folder
ENV S2I_HOME=/opt/kafka/s2i

# Copy S2I scripts
COPY ./s2i-scripts $S2I_HOME

# Copy Kafka Exporter scripts
COPY ./exporter-scripts $KAFKA_EXPORTER_HOME

WORKDIR $KAFKA_HOME

USER 1001
