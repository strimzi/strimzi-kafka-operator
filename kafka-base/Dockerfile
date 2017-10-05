FROM fedora:26

RUN dnf -y install java-1.8.0-openjdk gettext hostname && dnf clean all -y

# set Kafka home folder
ENV KAFKA_HOME=/opt/kafka

# Add qpidd group / user
RUN groupadd -g 1000050000 -r kafka && useradd -r -u 1000050000 -d $KAFKA_HOME  -m -g kafka kafka

# Use kafka user
USER kafka:kafka
WORKDIR $KAFKA_HOME

# Set Scala and Kafka version
ENV SCALA_VERSION=2.11
ENV KAFKA_VERSION=0.11.0.1

# Set Kafka MD5checksum
ENV CHECKSUM="C8FD6521EC8D414687C7471524009A8A  kafka_2.11-0.11.0.1.tgz"

# Set from build args
ARG version=latest
ENV VERSION ${version}

# downloading/extracting Apache Kafka
RUN curl -O http://www.eu.apache.org/dist/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz  \
    && echo $CHECKSUM > kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz.md5                                     \
    && md5sum --check kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz.md5                                       \
    && tar xvfz kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz -C $KAFKA_HOME --strip-components=1             \
    && rm -f kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz*
