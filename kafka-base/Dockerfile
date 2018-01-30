FROM centos:7

RUN yum -y install java-1.8.0-openjdk-headless gettext nmap-ncat && yum clean all -y

# set Kafka home folder
ENV KAFKA_HOME=/opt/kafka

# Add kafka group / user
RUN groupadd -r -g 1001 kafka && useradd -r -m -u 1001 -g kafka kafka

# Set Scala and Kafka version
ENV SCALA_VERSION=2.11
ENV KAFKA_VERSION=1.0.0
ENV JMX_EXPORTER_VERSION=0.1.0

# Set Kafka (SHA512) and Prometheus JMX exporter (SHA1) checksums
ENV KAFKA_CHECKSUM="b698028485d460c494af942b2dfa41c7342a2ad6052b543c95289cd1e832a1ebcf679b7e568dcabc342c799765337b94f91bc0cf0ee915534cdf82d1635a622a  kafka_2.11-1.0.0.tgz"
ENV JMX_EXPORTER_CHECKSUM="6ab370edccc2eeb3985f4c95769c26c090d0e052 jmx_prometheus_javaagent-0.1.0.jar"

# Set from build args
ARG version=latest
ENV VERSION ${version}

# Downloading/extracting Apache Kafka
RUN curl -O https://www.apache.org/dist/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz \
    && echo $KAFKA_CHECKSUM > kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz.sha512 \
    && sha512sum --check kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz.sha512 \
    && mkdir $KAFKA_HOME \
    && tar xvfz kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz -C $KAFKA_HOME --strip-components=1 \
    && rm -f kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz*

# Downloading Prometheus JMX exporter
RUN curl -O https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/$JMX_EXPORTER_VERSION/jmx_prometheus_javaagent-$JMX_EXPORTER_VERSION.jar \
    && echo $JMX_EXPORTER_CHECKSUM > jmx_prometheus_javaagent-$JMX_EXPORTER_VERSION.jar.sha1 \
    && sha1sum --check jmx_prometheus_javaagent-$JMX_EXPORTER_VERSION.jar.sha1 \
    && mkdir /opt/prometheus \
    && mkdir /opt/prometheus/config \
    && mv jmx_prometheus_javaagent-$JMX_EXPORTER_VERSION.jar /opt/prometheus/jmx_prometheus_javaagent.jar \
    && rm -rf jmx_prometheus_javaagent-$JMX_EXPORTER_VERSION.*

# exposed port for the JMX exporter
EXPOSE 9404

WORKDIR $KAFKA_HOME
