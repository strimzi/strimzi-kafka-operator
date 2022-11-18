######################################################################
# The JMX Trans image does not use the Strimzi base image because it does not work under Java 17.
# Instead it starts from the UBI base image and installs everything on its own:
#     * Java 11
#     * Tini
######################################################################
FROM registry.access.redhat.com/ubi8/ubi-minimal:latest

LABEL org.opencontainers.image.source='https://github.com/strimzi/strimzi-kafka-operator'

ARG JAVA_VERSION=11
ARG TARGETPLATFORM

USER root

RUN microdnf update \
    && microdnf --setopt=install_weak_deps=0 --setopt=tsflags=nodocs install java-${JAVA_VERSION}-openjdk-headless \
    && microdnf clean all

ENV JAVA_HOME /usr/lib/jvm/jre-11

#####
# Add Tini
#####
ENV TINI_VERSION v0.19.0
ENV TINI_SHA256_AMD64=93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c
ENV TINI_SHA256_ARM64=07952557df20bfd2a95f9bef198b445e006171969499a1d361bd9e6f8e5e0e81
ENV TINI_SHA256_PPC64LE=3f658420974768e40810001a038c29d003728c5fe86da211cff5059e48cfdfde
ENV TINI_SHA256_S390X=931b70a182af879ca249ae9de87ef68423121b38d235c78997fafc680ceab32d

RUN echo $TARGETPLATFORM

RUN set -ex; \
    if [[ ${TARGETPLATFORM} = "linux/ppc64le" ]]; then \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-ppc64le -o /usr/bin/tini; \
        echo "${TINI_SHA256_PPC64LE} */usr/bin/tini" | sha256sum -c; \
        chmod +x /usr/bin/tini; \
    elif [[ ${TARGETPLATFORM} = "linux/arm64" ]]; then \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-arm64 -o /usr/bin/tini; \
        echo "${TINI_SHA256_ARM64} */usr/bin/tini" | sha256sum -c; \
        chmod +x /usr/bin/tini; \
    elif [[ ${TARGETPLATFORM} = "linux/s390x" ]]; then \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-s390x -o /usr/bin/tini; \
        echo "${TINI_SHA256_S390X} */usr/bin/tini" | sha256sum -c; \
        chmod +x /usr/bin/tini; \
    else \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -o /usr/bin/tini; \
        echo "${TINI_SHA256_AMD64} */usr/bin/tini" | sha256sum -c; \
        chmod +x /usr/bin/tini; \
    fi

######################################################################
# This part of the Dockerfile adds the JMXTrans binaries and scripts
######################################################################

ENV JMXTRANS_HOME /usr/share/jmxtrans
ENV PATH $JMXTRANS_HOME/bin:$PATH
ENV JAR_FILE $JMXTRANS_HOME/lib/jmxtrans.jar
ENV JMXTRANS_VERSION 272
ENV JMXTRANS_CHECKSUM="834eef1957e553ad561cd0730f985570e68e0771be743172d371e89a3c6eeeb3d6dfee9a79b08987a4c0e269dd121ff96d24eeef39ff6982a6474f0675825dc6  jmxtrans-${JMXTRANS_VERSION}-all.jar"
ENV SECONDS_BETWEEN_RUNS 60
ENV CONTINUE_ON_ERROR false
ENV JSON_DIR /var/lib/jmxtrans

WORKDIR ${JMXTRANS_HOME}
RUN mkdir -p ${JMXTRANS_HOME}/conf

#####
# Add JmxTrans Jar
#####
RUN mkdir -p /usr/share/jmxtrans/lib/ \
    && mkdir -p /var/log/jmxtrans \
    && curl -k https://repo1.maven.org/maven2/org/jmxtrans/jmxtrans/${JMXTRANS_VERSION}/jmxtrans-${JMXTRANS_VERSION}-all.jar --output jmxtrans-${JMXTRANS_VERSION}-all.jar \
    && echo $JMXTRANS_CHECKSUM > jmxtrans-${JMXTRANS_VERSION}-all.jar.sha512 \
    && sha512sum --check jmxtrans-${JMXTRANS_VERSION}-all.jar.sha512 \
    && rm jmxtrans-${JMXTRANS_VERSION}-all.jar.sha512 \
    && mv jmxtrans-${JMXTRANS_VERSION}-all.jar ${JAR_FILE}

COPY docker-entrypoint.sh /docker-entrypoint.sh
COPY jmxtrans_readiness_check.sh /opt/jmx/
COPY logback.xml ${JMXTRANS_HOME}/conf/

#####
# Add NC
#####
RUN microdnf install nc \
    && microdnf clean all

VOLUME ${JSON_DIR}

ENTRYPOINT [ "/docker-entrypoint.sh",  "start-without-jmx" ]

USER 1001
