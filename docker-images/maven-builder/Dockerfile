FROM registry.access.redhat.com/ubi9/openjdk-17:1.18

LABEL org.opencontainers.image.source='https://github.com/strimzi/strimzi-kafka-operator'

USER root

# Add strimzi user with UID 1001
# The user is in the group 0 to have access to the mounted volumes and storage
RUN useradd -r -m -u 1001 -g 0 strimzi

RUN microdnf update -y \
    && microdnf clean all -y

USER 1001
