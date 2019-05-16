FROM centos:7

ARG JAVA_VERSION
ARG KAFKA_SHA512
ARG KAFKA_VERSION
ARG strimzi_version

RUN yum -y update \
    && yum -y install java-${JAVA_VERSION}-openjdk-headless gettext nmap-ncat openssl \
    && yum clean all -y

# set Kafka home folder
ENV KAFKA_HOME=/opt/kafka

# Add kafka user with UID 1001
# The user is in the group 0 to have access to the mounted volumes and storage
RUN useradd -r -m -u 1001 -g 0 kafka

# Set Scala and Kafka version
ENV KAFKA_VERSION=${KAFKA_VERSION}
ENV STRIMZI_VERSION=${strimzi_version}
ENV SCALA_VERSION=2.12
ENV JMX_EXPORTER_VERSION=0.1.0

# Set Kafka (SHA512) and Prometheus JMX exporter (SHA1) checksums
ENV KAFKA_CHECKSUM="${KAFKA_SHA512}  kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
ENV JMX_EXPORTER_CHECKSUM="6ab370edccc2eeb3985f4c95769c26c090d0e052 jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar"

# Downloading Prometheus JMX exporter
RUN curl -O https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar \
    && echo $JMX_EXPORTER_CHECKSUM > jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar.sha1 \
    && sha1sum --check jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar.sha1 \
    && mkdir -p /opt/prometheus/config \
    && mv jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar /opt/prometheus/jmx_prometheus_javaagent.jar \
    && rm -rf jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.*

# Downloading/extracting Apache Kafka
RUN curl -O https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz \
    && echo $KAFKA_CHECKSUM > kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz.sha512 \
    && sha512sum --check kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz.sha512 \
    && mkdir $KAFKA_HOME \
    && tar xvfz kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C $KAFKA_HOME --strip-components=1 \
    && rm -f kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz*

COPY ./tmp/kafka-agent-${strimzi_version}.jar ${KAFKA_HOME}/libs/

RUN yum -y install stunnel net-tools && yum clean all -y

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

WORKDIR $KAFKA_HOME

USER 1001
