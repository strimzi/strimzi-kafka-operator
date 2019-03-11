FROM centos:7
ARG JAVA_VERSION

RUN yum -y install java-${JAVA_VERSION}-openjdk-headless openssl && yum -y clean all

# Set JAVA_HOME env var
ENV JAVA_HOME /usr/lib/jvm/java

# Add strimzi user with UID 1001
# The user is in the group 0 to have access to the mounted volumes and storage
RUN useradd -r -m -u 1001 -g 0 strimzi

ARG strimzi_version=1.0-SNAPSHOT
ENV STRIMZI_VERSION ${strimzi_version}
ENV STRIMZI_HOME=/opt/strimzi
RUN mkdir -p /opt/strimzi/bin
WORKDIR /opt/strimzi

COPY scripts/ bin/

COPY lib/ lib/

USER 1001
