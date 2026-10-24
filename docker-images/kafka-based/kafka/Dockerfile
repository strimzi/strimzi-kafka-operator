#####
# Build the helper container image for downloading prerequisites
#####
FROM registry.access.redhat.com/ubi9/ubi-minimal:latest AS downloader

ARG TARGETOS
ARG TARGETARCH

RUN microdnf --setopt=install_weak_deps=0 --setopt=tsflags=nodocs install -y \
            # Tar is used to unpack the downloaded binaries here in the Dockerfile \
            tar \
            # gzip is used to unpack the downloaded binaries here in the Dockerfile \
            gzip

#####
# Get Kafka Exporter
#####
ENV KAFKA_EXPORTER_HOME=/opt/kafka-exporter
ENV KAFKA_EXPORTER_VERSION=1.11.0
ENV KAFKA_EXPORTER_CHECKSUM_AMD64="8836e92827132dd232010a1c53fc7e3a815b4c9e47b8b5791957c43ad1a839035482ea69aca42891e626b6a96bcd791cc863cc64315ce519d0ea4604090a3a93  kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz"
ENV KAFKA_EXPORTER_CHECKSUM_ARM64="8c5d3eadf086dea24c49962ef058a61b8457ec52b6173834b3b311066914874235de439aa2e97ad6e59bc95bba577bc3ebd663edc03afaa4440551b451d69f63  kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz"
ENV KAFKA_EXPORTER_CHECKSUM_PPC64LE="6a25e0b953dc3663b3439bf1f17bc525e7735d2de6cddda36e90f4cf3db8ea541f5e1b6647db678a61014e4ce216aa96ebd909c277a759c0cb1e4de31a565599  kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz"
ENV KAFKA_EXPORTER_CHECKSUM_S390X="876423ca7513ada825a17e657662bee1644b47471e61cc13758ed6296c32b34492b301c41e287a40ce674dd5cd71f43dc47bdac674484d87db14296221c4bb0d kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-s390x.tar.gz"

RUN set -ex; \
    if [[ "${TARGETOS}/${TARGETARCH}" = "linux/arm64" ]]; then \
        curl -LO https://github.com/danielqsj/kafka_exporter/releases/download/v${KAFKA_EXPORTER_VERSION}/kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz; \
        echo $KAFKA_EXPORTER_CHECKSUM_ARM64 > kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz.sha512; \
        sha512sum -c kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz.sha512; \
        mkdir $KAFKA_EXPORTER_HOME; \
        tar xvfz kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz -C $KAFKA_EXPORTER_HOME --strip-components=1; \
        rm -f kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-arm64.tar.gz*; \
    elif [[ "${TARGETOS}/${TARGETARCH}" = "linux/ppc64le" ]]; then \
        curl -LO https://github.com/danielqsj/kafka_exporter/releases/download/v${KAFKA_EXPORTER_VERSION}/kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz; \
        echo $KAFKA_EXPORTER_CHECKSUM_PPC64LE > kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz.sha512; \
        sha512sum -c kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz.sha512; \
        mkdir $KAFKA_EXPORTER_HOME; \
        tar xvfz kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz -C $KAFKA_EXPORTER_HOME --strip-components=1; \
        rm -f kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-ppc64le.tar.gz*; \
    elif [[ "${TARGETOS}/${TARGETARCH}" = "linux/s390x" ]]; then \
        curl -LO https://github.com/danielqsj/kafka_exporter/releases/download/v${KAFKA_EXPORTER_VERSION}/kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-s390x.tar.gz; \
        echo $KAFKA_EXPORTER_CHECKSUM_S390X > kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-s390x.tar.gz.sha512; \
        sha512sum -c kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-s390x.tar.gz.sha512; \
        mkdir $KAFKA_EXPORTER_HOME; \
        tar xvfz kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-s390x.tar.gz -C $KAFKA_EXPORTER_HOME --strip-components=1; \
        rm -f kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-s390x.tar.gz*; \
    else \
        curl -LO https://github.com/danielqsj/kafka_exporter/releases/download/v${KAFKA_EXPORTER_VERSION}/kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz; \
        echo $KAFKA_EXPORTER_CHECKSUM_AMD64 > kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz.sha512; \
        sha512sum -c kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz.sha512; \
        mkdir $KAFKA_EXPORTER_HOME; \
        tar xvfz kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz -C $KAFKA_EXPORTER_HOME --strip-components=1; \
        rm -f kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz*; \
    fi

#####
# Get Prometheus JMX Exporter
#####
ENV JMX_EXPORTER_HOME=/opt/prometheus-jmx-exporter
ENV JMX_EXPORTER_VERSION=1.7.0
ENV JMX_EXPORTER_CHECKSUM="7322be2ffd6c0d418622cd48a4dbd9fcd62eca7d163e4ff02992fdd907c1e2dc7dc330e7dd15548d755330c9c512773a8c0f89f9b6bb2b8826ecf6c539f25e0c  jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar"

RUN set -ex; \
    curl -LO https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar; \
    echo $JMX_EXPORTER_CHECKSUM > jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar.sha512; \
    sha512sum -c jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar.sha512; \
    mkdir $JMX_EXPORTER_HOME; \
    mv jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar $JMX_EXPORTER_HOME/; \
    rm -f jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar.sha512;

#####
# Stage 1: Establish clean ubi9-micro rootfs with its own RPM database
#####
FROM registry.access.redhat.com/ubi9/ubi-micro:latest AS kafka-base

#####
# Install Kafka-specific runtime tools into the ubi9-micro rootfs using dnf --installroot
#####
FROM registry.access.redhat.com/ubi9/ubi-minimal:latest AS kafka-tools

# Copy the ubi9-micro rootfs (including its RPM database) as the install root
COPY --from=kafka-base / /mnt/rootfs/

RUN microdnf install \
        --installroot /mnt/rootfs \
        --noplugins \
        --config /etc/dnf/dnf.conf \
        --setopt=cachedir=/var/cache/microdnf \
        --setopt=reposdir=/etc/yum.repos.d \
        --setopt=varsdir=/etc/dnf \
        --setopt=install_weak_deps=0 \
        --setopt=tsflags=nodocs \
        --releasever 9 \
        -y \
        # Required for network diagnostics (e.g. netstat)
        net-tools \
        # Required to resolve the pod hostname inside scripts
        hostname \
        # Required for script utilities (e.g. find)
        findutils \
        # Required to unpack Kafka and other archives
        tar \
        # Required to decompress .tar.gz archives
        gzip \
        # Required to unpack zip archives (e.g. Kafka plugins)
        unzip \
        # Required to download files from the internet (e.g. connectors)
        curl-minimal \
    && microdnf \
        --installroot /mnt/rootfs \
        --noplugins \
        --config /etc/dnf/dnf.conf \
        --setopt=cachedir=/var/cache/microdnf \
        --setopt=reposdir=/etc/yum.repos.d \
        --setopt=varsdir=/etc/dnf \
        clean all

#####
# Build the main container image
#####
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

#####
# Copy Kafka-specific tools from chroot
#####
COPY --from=kafka-tools /mnt/rootfs /

# Add kafka user with UID 1001
# The user is in the group 0 to have access to the mounted volumes and storage
RUN echo "kafka:x:1001:0::/home/kafka:/bin/false" >> /etc/passwd

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
# Copy Kafka Exporter from the downloader stage and the helper scripts from local filesystem
#####
ENV KAFKA_EXPORTER_HOME=/opt/kafka-exporter
COPY --from=downloader $KAFKA_EXPORTER_HOME/kafka_exporter $KAFKA_EXPORTER_HOME/kafka_exporter
COPY ./exporter-scripts $KAFKA_EXPORTER_HOME

#####
# Copy Prometheus JMX Exporter from the downloader stage
#####
ENV JMX_EXPORTER_HOME=/opt/prometheus-jmx-exporter
COPY --from=downloader $JMX_EXPORTER_HOME/jmx_prometheus_javaagent*.jar $JMX_EXPORTER_HOME/

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
