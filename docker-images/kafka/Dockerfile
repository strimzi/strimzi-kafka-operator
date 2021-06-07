FROM strimzi/base:latest

ARG KAFKA_DIST_DIR
ARG KAFKA_VERSION
ARG THIRD_PARTY_LIBS
ARG strimzi_version
ARG TARGETPLATFORM

RUN yum -y install gettext nmap-ncat stunnel net-tools && yum clean all -y
RUN yum -y install unzip && yum clean all -y

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
ENV KAFKA_EXPORTER_VERSION=1.2.0
ENV KAFKA_EXPORTER_CHECKSUM_AMD64="7afa40365ddf0cb0a88457684bd64d565e250c7e5a4536ba7f9d37d02d2808c3b07766f94e0b1338beb296573ade29db630948c931be44bde416c0410b5d783b  kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz"
ENV KAFKA_EXPORTER_CHECKSUM_ARM64="1b36d0dc45b9dc20d90f88ee43b84277154d6d589c19b0674b10b4c3e48ce31e5410094deeae4426e069f9d30108bb26dcb84fab2e87e2841dadb75e58fb5916  kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz"

RUN set -ex; \
    if [[ ${TARGETPLATFORM} = "linux/arm64" ]]; then \
        curl -kLO https://repo.phenix.carrefour.com/common/kafka/kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz; \
        echo $KAFKA_EXPORTER_CHECKSUM_ARM64 > kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz.sha512; \
        #sha512sum --check kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz.sha512; \
        mkdir $KAFKA_EXPORTER_HOME; \
        tar xvfz kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz -C $KAFKA_EXPORTER_HOME --strip-components=1; \
        rm -f kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz*; \
    else \
        curl -kLO https://repo.phenix.carrefour.com/common/kafka/kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz; \
        echo $KAFKA_EXPORTER_CHECKSUM_AMD64 > kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz.sha512; \
        #sha512sum --check kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz.sha512; \
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

#####
# Add Kafka Connect S2I feature
#####
ENV S2I_HOME=/opt/kafka/s2i
COPY ./s2i-scripts $S2I_HOME

WORKDIR $KAFKA_HOME

USER 1001
