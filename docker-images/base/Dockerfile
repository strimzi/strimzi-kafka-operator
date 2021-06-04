FROM centos:7
ARG JAVA_VERSION=11
ARG TARGETPLATFORM

RUN yum -y update \
    && yum -y install java-${JAVA_VERSION}-openjdk-headless openssl bind-utils \
    && yum -y clean all

#####
# Add Tini
#####
ENV TINI_VERSION v0.18.0
ENV TINI_SHA256_AMD64=12d20136605531b09a2c2dac02ccee85e1b874eb322ef6baf7561cd93f93c855
ENV TINI_SHA256_ARM64=7c5463f55393985ee22357d976758aaaecd08defb3c5294d353732018169b019

RUN set -ex; \
    if [[ ${TARGETPLATFORM} = "linux/arm64" ]]; then \
        curl -ks -L https://repo.phenix.carrefour.com/common/tini/tini-arm64.v0.18.0 -o /usr/bin/tini; \
        #echo "${TINI_SHA256_ARM64} */usr/bin/tini" | sha256sum -c; \
        chmod +x /usr/bin/tini; \
    else \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -o /usr/bin/tini; \
        echo "${TINI_SHA256_AMD64} */usr/bin/tini" | sha256sum -c; \
        chmod +x /usr/bin/tini; \
    fi