FROM registry.access.redhat.com/ubi8/ubi-minimal:latest

LABEL org.opencontainers.image.source='https://github.com/strimzi/strimzi-kafka-operator'

ARG JAVA_VERSION=17
ARG TARGETOS
ARG TARGETARCH

USER root

RUN microdnf update \
    && microdnf --setopt=install_weak_deps=0 --setopt=tsflags=nodocs install java-${JAVA_VERSION}-openjdk-headless openssl shadow-utils \
    && microdnf reinstall -y tzdata \
    && microdnf clean all

ENV JAVA_HOME /usr/lib/jvm/jre-17

#####
# Add Tini
#####
ENV TINI_VERSION v0.19.0
ENV TINI_SHA256_AMD64=93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c
ENV TINI_SHA256_ARM64=07952557df20bfd2a95f9bef198b445e006171969499a1d361bd9e6f8e5e0e81
ENV TINI_SHA256_PPC64LE=3f658420974768e40810001a038c29d003728c5fe86da211cff5059e48cfdfde
ENV TINI_SHA256_S390X=931b70a182af879ca249ae9de87ef68423121b38d235c78997fafc680ceab32d

RUN set -ex; \
    if [[ "${TARGETOS}/${TARGETARCH}" = "linux/ppc64le" ]]; then \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-ppc64le -o /usr/bin/tini; \
        echo "${TINI_SHA256_PPC64LE} */usr/bin/tini" | sha256sum -c; \
        chmod +x /usr/bin/tini; \
    elif [[ "${TARGETOS}/${TARGETARCH}" = "linux/arm64" ]]; then \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-arm64 -o /usr/bin/tini; \
        echo "${TINI_SHA256_ARM64} */usr/bin/tini" | sha256sum -c; \
        chmod +x /usr/bin/tini; \
    elif [[ "${TARGETOS}/${TARGETARCH}" = "linux/s390x" ]]; then \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-s390x -o /usr/bin/tini; \
        echo "${TINI_SHA256_S390X} */usr/bin/tini" | sha256sum -c; \
        chmod +x /usr/bin/tini; \
    else \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -o /usr/bin/tini; \
        echo "${TINI_SHA256_AMD64} */usr/bin/tini" | sha256sum -c; \
        chmod +x /usr/bin/tini; \
    fi
