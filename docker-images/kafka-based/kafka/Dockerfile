FROM strimzi/base:latest

LABEL org.opencontainers.image.source='https://github.com/strimzi/strimzi-kafka-operator'

ARG KAFKA_DIST_DIR
ARG KAFKA_VERSION
ARG THIRD_PARTY_LIBS
ARG strimzi_version
ARG TARGETOS
ARG TARGETARCH

LABEL name='kafka' \
    version="${strimzi_version}-${KAFKA_VERSION}" \
    release="${strimzi_version}" \
    summary="Kafka ${KAFKA_VERSION} image of the Strimzi Kafka Operator project." \
    description="Kafka image used by the Strimzi Kafka Operator to run Kafka ${KAFKA_VERSION} clusters."

RUN microdnf --setopt=install_weak_deps=0 --setopt=tsflags=nodocs install -y gettext nmap-ncat net-tools unzip hostname findutils tar \
    && microdnf clean all -y

# Add kafka user with UID 1001
# The user is in the group 0 to have access to the mounted volumes and storage
RUN useradd -r -m -u 1001 -g 0 kafka

#####
# Add Kafka
#####
ENV KAFKA_HOME=/opt/kafka
ENV KAFKA_VERSION=${KAFKA_VERSION}
ENV STRIMZI_VERSION=${strimzi_version}

COPY $KAFKA_DIST_DIR $KAFKA_HOME
COPY ./scripts/ $KAFKA_HOME
RUN mkdir $KAFKA_HOME/plugins

#####
# Add Kafka Exporter
#####
ENV KAFKA_EXPORTER_HOME=/opt/kafka-exporter
RUN mkdir $KAFKA_EXPORTER_HOME
COPY kafka_exporter $KAFKA_EXPORTER_HOME
COPY ./exporter-scripts $KAFKA_EXPORTER_HOME

#####
# Add Prometheus JMX Exporter
#####
ENV JMX_EXPORTER_HOME=/opt/prometheus-jmx-exporter
ENV JMX_EXPORTER_VERSION=1.5.0
ENV JMX_EXPORTER_CHECKSUM="76e09532d01ea38c5718d77bee0da7ab628b9f813d85cc59928c9a1dde0f0f864b1e52c0494298e738ab709a31e8d0fa5318bfa1825a82157c7d3c7b9fa33c0a  jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar"

RUN set -ex; \
    curl -LO https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar; \
    echo $JMX_EXPORTER_CHECKSUM > jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar.sha512; \
    sha512sum --check jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar.sha512; \
    mkdir $JMX_EXPORTER_HOME; \
    mv jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar $JMX_EXPORTER_HOME/; \
    rm -f jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar.sha512;

#####
# Add Strimzi agents
#####
COPY ./tmp/kafka-agent-${STRIMZI_VERSION}.jar ${KAFKA_HOME}/libs/
COPY ./tmp/tracing-agent-${STRIMZI_VERSION}.jar ${KAFKA_HOME}/libs/

#####
# Add 3rd party libs
#####
COPY tmp/thirdparty-libs-${THIRD_PARTY_LIBS}/ ${KAFKA_HOME}/libs/

#####
# Add Cruise Control
#####
ENV CRUISE_CONTROL_HOME=/opt/cruise-control
RUN mkdir $CRUISE_CONTROL_HOME
COPY tmp/cc/ ${CRUISE_CONTROL_HOME}/libs/
COPY ./cruise-control-scripts $CRUISE_CONTROL_HOME

WORKDIR $KAFKA_HOME

USER 1001
