FROM strimzi/base:latest

ARG KAFKA_DIST_DIR
ARG KAFKA_VERSION
ARG THIRD_PARTY_LIBS
ARG strimzi_version
ARG TARGETPLATFORM

RUN microdnf install gettext nmap-ncat stunnel net-tools unzip hostname findutils tar \
    && microdnf clean all

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

#####
# Add Kafka Exporter
#####
ENV KAFKA_EXPORTER_HOME=/opt/kafka-exporter
ENV KAFKA_EXPORTER_VERSION=1.3.1-STRIMZI
ENV KAFKA_EXPORTER_CHECKSUM_AMD64="85e37fe8a7797f53dcf1ef349b3472edc6891d8bb914d1aebb33784bfb850189d47ec989be9a8c764f4fbe991576b81545b04ddbd4ff6946a677066ec0a4619d  kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz"
ENV KAFKA_EXPORTER_CHECKSUM_ARM64="a594903265f3497c003d90e211480179aa8d42fb58b43456f001d3eea064d1d571e3b5bb9666c6d45382b1611433c5d616d68b742f84045be04c0c18b9df0427  kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz"
ENV KAFKA_EXPORTER_CHECKSUM_PPC64LE="8b72420d2c6aed25b6ddbae7df66be6a07e659fffa6b3f6cae1132de35c7f0a21bde0fcb3fa9234a8a79839589c18940ef01534551b57669dab09544b5af2883  kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz"

RUN set -ex; \
    if [[ ${TARGETPLATFORM} = "linux/arm64" ]]; then \
        curl -LO https://github.com/alesj/kafka_exporter/releases/download/v${KAFKA_EXPORTER_VERSION}/kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz; \
        echo $KAFKA_EXPORTER_CHECKSUM_ARM64 > kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz.sha512; \
        sha512sum --check kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz.sha512; \
        mkdir $KAFKA_EXPORTER_HOME; \
        tar xvfz kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz -C $KAFKA_EXPORTER_HOME --strip-components=1; \
        rm -f kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz*; \
    elif [[ ${TARGETPLATFORM} = "linux/ppc64le" ]]; then \
        curl -LO https://github.com/alesj/kafka_exporter/releases/download/v${KAFKA_EXPORTER_VERSION}/kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz; \
        echo $KAFKA_EXPORTER_CHECKSUM_PPC64LE > kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz.sha512; \
        sha512sum --check kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz.sha512; \
        mkdir $KAFKA_EXPORTER_HOME; \
        tar xvfz kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz -C $KAFKA_EXPORTER_HOME --strip-components=1; \
        rm -f kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz*; \
    else \
        curl -LO https://github.com/alesj/kafka_exporter/releases/download/v${KAFKA_EXPORTER_VERSION}/kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz; \
        echo $KAFKA_EXPORTER_CHECKSUM_AMD64 > kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz.sha512; \
        sha512sum --check kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz.sha512; \
        mkdir $KAFKA_EXPORTER_HOME; \
        tar xvfz kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz -C $KAFKA_EXPORTER_HOME --strip-components=1; \
        rm -f kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz*; \
    fi

COPY ./exporter-scripts $KAFKA_EXPORTER_HOME

#####
# Add Strimzi agents
#####
COPY ./tmp/kafka-agent-${STRIMZI_VERSION}.jar ${KAFKA_HOME}/libs/
COPY ./tmp/mirror-maker-agent-${STRIMZI_VERSION}.jar ${KAFKA_HOME}/libs/
COPY ./tmp/tracing-agent-${STRIMZI_VERSION}.jar ${KAFKA_HOME}/libs/

#####
# Add 3rd party libs
#####
COPY kafka-thirdparty-libs/${THIRD_PARTY_LIBS}/target/dependency/ ${KAFKA_HOME}/libs/

#####
# Add Cruise Control
#####
ENV CRUISE_CONTROL_HOME=/opt/cruise-control
RUN mkdir $CRUISE_CONTROL_HOME
COPY kafka-thirdparty-libs/cc/target/dependency/ ${CRUISE_CONTROL_HOME}/libs/
COPY ./cruise-control-scripts $CRUISE_CONTROL_HOME

#####
# Add Stunnel
#####
ENV STUNNEL_HOME=/opt/stunnel
RUN mkdir $STUNNEL_HOME && mkdir -p -m g+rw /usr/local/var/run/
COPY ./stunnel-scripts/ $STUNNEL_HOME

WORKDIR $KAFKA_HOME

USER 1001
