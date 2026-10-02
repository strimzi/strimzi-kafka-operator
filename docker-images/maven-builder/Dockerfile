#####
# Stage 1: Establish clean ubi9-micro rootfs with its own RPM database
#####
FROM registry.access.redhat.com/ubi9/ubi-micro:latest AS base

#####
# Stage 2: Install runtime dependencies into the ubi9-micro rootfs using dnf --installroot
#####
FROM registry.access.redhat.com/ubi9/ubi-minimal:latest AS builder

ARG JAVA_VERSION=21

# Copy the ubi9-micro rootfs (including its RPM database) as the install root
COPY --from=base / /mnt/rootfs/

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
        java-${JAVA_VERSION}-openjdk-headless \
        maven \
        curl-minimal \
        bash \
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
# Stage 3: Build the final container image on ubi9-micro base
#####
FROM registry.access.redhat.com/ubi9/ubi-micro:latest

LABEL org.opencontainers.image.source='https://github.com/strimzi/strimzi-kafka-operator'

ARG strimzi_version

LABEL name='maven-builder' \
    vendor='Strimzi' \
    version="${strimzi_version}" \
    release="${strimzi_version}" \
    summary='Maven builder image of the Strimzi Kafka Operator.' \
    description='Builder image used to dynamically create Kafka Connect images with user-provided plugins.'

COPY --from=builder /mnt/rootfs /

RUN echo "strimzi:x:1001:0::/home/strimzi:/bin/false" >> /etc/passwd && \
    mkdir -p /home/strimzi && \
    chown 1001:0 /home/strimzi && \
    chmod 770 /home/strimzi

USER 1001
