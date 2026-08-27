FROM registry.ntrlab.ru/cbr/axiom-jdk-alpine-musl:17

ARG TARGETOS
ARG TARGETARCH
ARG strimzi_version=1.0-SNAPSHOT

LABEL name='strimzi-base' \
      summary='Base image for Strimzi Kafka'

USER root

RUN ln -sf /usr/lib/jvm/jdk-17.0.4.1-bellsoft-x86_64 /usr/lib/jvm/java-17-openjdk

# Устанавливаем необходимые пакеты для работы и распаковки
RUN apk update && apk upgrade --no-cache
RUN apk add --no-cache \
      openssl \
      shadow \
      unzip \
      curl \
      bash \
      tzdata \
      gettext \
      nmap \
      net-tools \
      findutils \
      grep \
      sed \
      gawk \
      tar \
    && cp /usr/share/zoneinfo/UTC /etc/localtime \
    && echo "UTC" > /etc/timezone
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH="$JAVA_HOME/bin:$PATH"

ENV TINI_VERSION=v0.19.0
ENV TINI_SHA256_AMD64=93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c
ENV TINI_SHA256_ARM64=07952557df20bfd2a95f9bef198b446.106171969499a1d361bd9e6f8e5e0e81
ENV TINI_SHA256_PPC64LE=3f658420974768e40810001a038c29d003728c5fe86da211cff5059e48cfdfde
ENV TINI_SHA256_S390X=931b70a182af879ca249ae9de87ef68423121b38d235c78997fafc680ceab32d

# Установка tini
RUN apk add --no-cache tini

RUN ln -s /sbin/tini /usr/bin/tini

##RUN sed -i 's|/usr/bin/tini|/sbin/tini|' /opt/strimzi/bin/launch_java.sh

