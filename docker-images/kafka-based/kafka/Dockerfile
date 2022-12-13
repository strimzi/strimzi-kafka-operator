FROM strimzi/base:latest

LABEL org.opencontainers.image.source='https://github.com/strimzi/strimzi-kafka-operator'

ARG KAFKA_DIST_DIR
ARG KAFKA_VERSION
ARG THIRD_PARTY_LIBS
ARG strimzi_version
ARG TARGETPLATFORM

RUN microdnf --setopt=install_weak_deps=0 --setopt=tsflags=nodocs install gettext nmap-ncat stunnel net-tools unzip hostname findutils tar \
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
ENV KAFKA_EXPORTER_VERSION=1.6.0
ENV KAFKA_EXPORTER_CHECKSUM_AMD64="18a52a92f8c1ef06175f2cb7e12b81aed75b4a87884fb3c3120c6671e2b25e3a3b52dda02eb952ba22f892598c8a47c474b625d481701d83382650c8b4616550  kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz"
ENV KAFKA_EXPORTER_CHECKSUM_ARM64="6ad7e142830a3326b109f01fb6f706fe10afb5b32722c3c73dd98da5196df49efa67e342115d45b07284bc9a227b08ee68b9e959863fe79fbb57596371410809  kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz"
ENV KAFKA_EXPORTER_CHECKSUM_PPC64LE="0eaee79659b0988d65f61a93e5f4df35caa9a74eaa6922eb2b5fa5094d46a2c608c51d166ca98afaed94b702a2ea749f348c01ebb2680d253cdc3b59cbc99c00  kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz"
ENV KAFKA_EXPORTER_CHECKSUM_S390X="2a4bf6ad41941d62341c3b13dd05dc115c92461d3ae664809c7b3afbec2f90fd01c561be327fb96e9aa4a7f6e7b10830ceac059cb7801fef557124b803d07b4f kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-s390x.tar.gz"

RUN set -ex; \
    if [[ ${TARGETPLATFORM} = "linux/arm64" ]]; then \
        curl -LO https://github.com/danielqsj/kafka_exporter/releases/download/v${KAFKA_EXPORTER_VERSION}/kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz; \
        echo $KAFKA_EXPORTER_CHECKSUM_ARM64 > kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz.sha512; \
        sha512sum --check kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz.sha512; \
        mkdir $KAFKA_EXPORTER_HOME; \
        tar xvfz kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz -C $KAFKA_EXPORTER_HOME --strip-components=1; \
        rm -f kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz*; \
    elif [[ ${TARGETPLATFORM} = "linux/ppc64le" ]]; then \
        curl -LO https://github.com/danielqsj/kafka_exporter/releases/download/v${KAFKA_EXPORTER_VERSION}/kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz; \
        echo $KAFKA_EXPORTER_CHECKSUM_PPC64LE > kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz.sha512; \
        sha512sum --check kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz.sha512; \
        mkdir $KAFKA_EXPORTER_HOME; \
        tar xvfz kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz -C $KAFKA_EXPORTER_HOME --strip-components=1; \
        rm -f kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz*; \
    elif [[ ${TARGETPLATFORM} = "linux/s390x" ]]; then \
        curl -LO https://github.com/danielqsj/kafka_exporter/releases/download/v${KAFKA_EXPORTER_VERSION}/kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-s390x.tar.gz; \
        echo $KAFKA_EXPORTER_CHECKSUM_S390X > kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-s390x.tar.gz.sha512; \
        sha512sum --check kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-s390x.tar.gz.sha512; \
        mkdir $KAFKA_EXPORTER_HOME; \
        tar xvfz kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-s390x.tar.gz -C $KAFKA_EXPORTER_HOME --strip-components=1; \
        rm -f kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-s390x.tar.gz*; \
    else \
        curl -LO https://github.com/danielqsj/kafka_exporter/releases/download/v${KAFKA_EXPORTER_VERSION}/kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz; \
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
COPY tmp/thirdparty-libs-${THIRD_PARTY_LIBS}/ ${KAFKA_HOME}/libs/

#####
# Add Cruise Control
#####
ENV CRUISE_CONTROL_HOME=/opt/cruise-control
RUN mkdir $CRUISE_CONTROL_HOME
COPY tmp/cc/ ${CRUISE_CONTROL_HOME}/libs/
COPY ./cruise-control-scripts $CRUISE_CONTROL_HOME

#####
# Add Stunnel
#####
ENV STUNNEL_HOME=/opt/stunnel
RUN mkdir $STUNNEL_HOME && mkdir -p -m g+rw /usr/local/var/run/
COPY ./stunnel-scripts/ $STUNNEL_HOME

WORKDIR $KAFKA_HOME

USER 1001
