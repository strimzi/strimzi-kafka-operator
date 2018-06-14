FROM centos:7

RUN yum -y install java-1.8.0-openjdk-headless gettext nmap-ncat openssl && yum clean all -y

# set Kafka home folder
ENV KAFKA_HOME=/opt/kafka

# Add kafka group / user
RUN groupadd -r -g 1001 kafka && useradd -r -m -u 1001 -g kafka kafka

# Set Scala and Kafka version
ENV SCALA_VERSION=2.12
ENV KAFKA_VERSION=1.1.0
ENV JMX_EXPORTER_VERSION=0.1.0

# Set Kafka (SHA512) and Prometheus JMX exporter (SHA1) checksums
ENV KAFKA_CHECKSUM="48d1ddc71f5a5b1b25d111f792553be69be62293640a3c6af985203c6ee88c6aa78e01327066bfad3feae6b0b45d71c0cac6ebd2d08843d92269132741a3791b  kafka_2.12-1.1.0.tgz"
ENV JMX_EXPORTER_CHECKSUM="6ab370edccc2eeb3985f4c95769c26c090d0e052 jmx_prometheus_javaagent-0.1.0.jar"

# Downloading/extracting Apache Kafka
RUN curl -O https://www.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz \
    && echo $KAFKA_CHECKSUM > kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz.sha512 \
    && sha512sum --check kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz.sha512 \
    && mkdir $KAFKA_HOME \
    && tar xvfz kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C $KAFKA_HOME --strip-components=1 \
    && rm -f kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz*

# Downloading Prometheus JMX exporter
RUN curl -O https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar \
    && echo $JMX_EXPORTER_CHECKSUM > jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar.sha1 \
    && sha1sum --check jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar.sha1 \
    && mkdir /opt/prometheus \
    && mkdir /opt/prometheus/config \
    && mv jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar /opt/prometheus/jmx_prometheus_javaagent.jar \
    && rm -rf jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.*

# copy scripts for starting Kafka
COPY ./scripts/ $KAFKA_HOME

WORKDIR $KAFKA_HOME
