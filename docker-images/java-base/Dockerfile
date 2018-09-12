FROM centos:7

RUN yum -y install java-1.8.0-openjdk-headless openssl && yum -y clean all

ARG HOME=/home/strimzi
ARG USER=strimzi

# Environment variables
ENV \
    HOME="${HOME}" \
    USER="${USER}" \
    JAVA_HOME="/usr/lib/jvm/java"

# Copy scripts for setting up user
COPY modules/common/add_user.sh /bin/
# Copy scripts for starting Java apps
COPY modules/java-base/scripts/* /bin/

# Custom scripts
RUN ["bash", "-x", "/bin/add_user.sh" ]

WORKDIR ${HOME}
