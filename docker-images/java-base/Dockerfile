FROM centos:7

RUN yum -y install java-1.8.0-openjdk-headless openssl && yum -y clean all

# Set JAVA_HOME env var
ENV JAVA_HOME /usr/lib/jvm/java

# Add strimzi user with UID 1001
# The user is in the group 0 to have access to the mounted volumes and storage
RUN useradd -r -m -u 1001 -g 0 strimzi

# Copy scripts for starting Java apps
COPY scripts/* /bin/