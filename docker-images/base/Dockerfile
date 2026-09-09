#####
# Install runtime dependencies into a chroot and download prerequisites
#####
FROM registry.access.redhat.com/ubi10/ubi-minimal:latest AS builder

ARG JAVA_VERSION=21
ARG TARGETOS
ARG TARGETARCH

RUN mkdir -p /mnt/rootfs && \
    microdnf install \
        --installroot /mnt/rootfs \
        --noplugins \
        --config /etc/dnf/dnf.conf \
        --setopt=cachedir=/var/cache/microdnf \
        --setopt=reposdir=/etc/yum.repos.d \
        --setopt=varsdir=/etc/dnf \
        --setopt=install_weak_deps=0 \
        --setopt=tsflags=nodocs \
        --releasever 10 \
        -y \
        # Java runtime
        java-${JAVA_VERSION}-openjdk-headless \
        # Required for TLS operations
        openssl \
        # Required for entrypoint scripts
        bash \
        # Required for timezone configuration
        tzdata \
    && microdnf \
        --installroot /mnt/rootfs \
        --noplugins \
        --config /etc/dnf/dnf.conf \
        --setopt=cachedir=/var/cache/microdnf \
        --setopt=reposdir=/etc/yum.repos.d \
        --setopt=varsdir=/etc/dnf \
        clean all

#####
# Get Tini
#####
ENV TINI_VERSION=v0.19.0
ENV TINI_SHA256_AMD64=93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c
ENV TINI_SHA256_ARM64=07952557df20bfd2a95f9bef198b445e006171969499a1d361bd9e6f8e5e0e81
ENV TINI_SHA256_PPC64LE=3f658420974768e40810001a038c29d003728c5fe86da211cff5059e48cfdfde
ENV TINI_SHA256_S390X=931b70a182af879ca249ae9de87ef68423121b38d235c78997fafc680ceab32d

RUN set -ex; \
    mkdir -p /mnt/rootfs/usr/bin; \
    if [[ "${TARGETOS}/${TARGETARCH}" = "linux/ppc64le" ]]; then \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-ppc64le -o /mnt/rootfs/usr/bin/tini; \
        echo "${TINI_SHA256_PPC64LE} */mnt/rootfs/usr/bin/tini" | sha256sum -c; \
        chmod +x /mnt/rootfs/usr/bin/tini; \
    elif [[ "${TARGETOS}/${TARGETARCH}" = "linux/arm64" ]]; then \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-arm64 -o /mnt/rootfs/usr/bin/tini; \
        echo "${TINI_SHA256_ARM64} */mnt/rootfs/usr/bin/tini" | sha256sum -c; \
        chmod +x /mnt/rootfs/usr/bin/tini; \
    elif [[ "${TARGETOS}/${TARGETARCH}" = "linux/s390x" ]]; then \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-s390x -o /mnt/rootfs/usr/bin/tini; \
        echo "${TINI_SHA256_S390X} */mnt/rootfs/usr/bin/tini" | sha256sum -c; \
        chmod +x /mnt/rootfs/usr/bin/tini; \
    else \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -o /mnt/rootfs/usr/bin/tini; \
        echo "${TINI_SHA256_AMD64} */mnt/rootfs/usr/bin/tini" | sha256sum -c; \
        chmod +x /mnt/rootfs/usr/bin/tini; \
    fi

#####
# Build the main container image on distroless base
#####
FROM registry.access.redhat.com/ubi10/ubi-micro:latest

LABEL org.opencontainers.image.source='https://github.com/strimzi/strimzi-kafka-operator'

ARG JAVA_VERSION=21
ARG strimzi_version

LABEL name='base' \
    vendor='Strimzi' \
    version="${strimzi_version}" \
    release="${strimzi_version}" \
    summary='Base image of the Strimzi Kafka Operator project.' \
    description='Strimzi Kafka Operator base image, containing common utilities across all component images.'

USER root

#####
# Copy installed packages from builder chroot
#####
COPY --from=builder /mnt/rootfs /

ENV JAVA_HOME=/usr/lib/jvm/jre-${JAVA_VERSION}
