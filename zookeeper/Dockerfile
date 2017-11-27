FROM fedora:26

RUN dnf -y install java-1.8.0-openjdk-headless gettext hostname nmap-ncat && dnf clean all -y

# set Kafka home folder
ENV ZOOKEEPER_HOME=/opt/zookeeper

# Add zookeeper group / user
RUN groupadd -r -g 1001 zookeeper && useradd -r -m -u 1001 -g zookeeper zookeeper

# Set Zookeeper version
ENV ZOOKEEPER_VERSION=3.4.10
ENV JMX_EXPORTER_VERSION=0.1.0

# Set Zookeeper and Prometheus JMX exporter (SHA1) checksums
ENV ZOOKEEPER_CHECKSUM="eb2145498c5f7a0d23650d3e0102318363206fba  zookeeper-3.4.10.tar.gz"
ENV JMX_EXPORTER_CHECKSUM="6ab370edccc2eeb3985f4c95769c26c090d0e052 jmx_prometheus_javaagent-0.1.0.jar"

# Set from build args
ARG version=latest
ENV VERSION ${version}

# downloading/extracting Apache Zookeeper
RUN curl -O https://www.apache.org/dist/zookeeper/zookeeper-$ZOOKEEPER_VERSION/zookeeper-$ZOOKEEPER_VERSION.tar.gz \
    && echo $ZOOKEEPER_CHECKSUM > zookeeper-$ZOOKEEPER_VERSION.tar.gz.md5 \
    && sha1sum --check zookeeper-$ZOOKEEPER_VERSION.tar.gz.md5 \
    && mkdir $ZOOKEEPER_HOME \
    && tar xvfz zookeeper-$ZOOKEEPER_VERSION.tar.gz -C $ZOOKEEPER_HOME --strip-components=1 \
    && rm -f zookeeper-$ZOOKEEPER_VERSION.tar.gz*

# Downloading Prometheus JMX exporter
RUN curl -O https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/$JMX_EXPORTER_VERSION/jmx_prometheus_javaagent-$JMX_EXPORTER_VERSION.jar \
    && echo $JMX_EXPORTER_CHECKSUM > jmx_prometheus_javaagent-$JMX_EXPORTER_VERSION.jar.sha1 \
    && sha1sum --check jmx_prometheus_javaagent-$JMX_EXPORTER_VERSION.jar.sha1 \
    && mkdir /opt/prometheus \
    && mv jmx_prometheus_javaagent-$JMX_EXPORTER_VERSION.jar /opt/prometheus/jmx_prometheus_javaagent.jar \
    && rm -rf jmx_prometheus_javaagent-$JMX_EXPORTER_VERSION.*

EXPOSE 2181 2888 3888 9404

WORKDIR $ZOOKEEPER_HOME

# copy template Zookeeper configuration files
COPY ./config/ $ZOOKEEPER_HOME/conf/

# copy scripts for starting Zookeeper
COPY ./scripts/ $ZOOKEEPER_HOME

USER zookeeper:zookeeper

ENTRYPOINT ["/opt/zookeeper/zookeeper_run.sh"]
