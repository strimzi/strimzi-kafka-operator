FROM fedora:26

RUN dnf -y install java-1.8.0-openjdk-headless gettext hostname && dnf clean all -y

# set Kafka home folder
ENV KAFKA_HOME=/opt/kafka

# Add kafka group / user
RUN groupadd -r -g 1001 kafka && useradd -r -m -u 1001 -g kafka kafka

# Set Scala and Kafka version
ENV SCALA_VERSION=2.11
ENV KAFKA_VERSION=1.0.0

# Set Kafka SHA512 checksum
ENV CHECKSUM="b698028485d460c494af942b2dfa41c7342a2ad6052b543c95289cd1e832a1ebcf679b7e568dcabc342c799765337b94f91bc0cf0ee915534cdf82d1635a622a  kafka_2.11-1.0.0.tgz"

# Set from build args
ARG version=latest
ENV VERSION ${version}

WORKDIR $KAFKA_HOME

# Downloading/extracting Apache Kafka
RUN curl -O https://www.apache.org/dist/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz \
    && echo $CHECKSUM > kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz.sha512 \
    && sha512sum --check kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz.sha512 \
    && tar xvfz kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz -C $KAFKA_HOME --strip-components=1 \
    && rm -f kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz*

COPY ./config/ $KAFKA_HOME/config/
