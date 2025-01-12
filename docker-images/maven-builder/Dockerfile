FROM registry.access.redhat.com/ubi9/openjdk-17:1.21

LABEL org.opencontainers.image.source='https://github.com/strimzi/strimzi-kafka-operator'

ARG strimzi_version

LABEL name='maven-builder' \
    vendor='Strimzi' \
    version="${strimzi_version}" \
    release="${strimzi_version}" \
    summary='Maven builder image of the Strimzi Kafka Operator.' \
    description='Builder image used to dynamically create Kafka Connect images with user-provided plugins.'

USER root

# Add strimzi user with UID 1001
# The user is in the group 0 to have access to the mounted volumes and storage
RUN useradd -r -m -u 1001 -g 0 strimzi

RUN microdnf update -y \
    && microdnf clean all -y

USER 1001
