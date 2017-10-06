FROM fedora:25

RUN dnf -y install which java-1.8.0-openjdk libaio python gettext hostname iputils wget && dnf clean all -y

# set Kafka home folder
ENV KAFKA_HOME=/opt/kafka

# Set from build args
ARG version=latest
ENV VERSION ${version}

VOLUME $KAFKA_HOME/plugins-ext

WORKDIR $KAFKA_HOME

# copy scripts for starting Kafka Connect
COPY ./scripts/ $KAFKA_HOME
COPY ./bin $KAFKA_HOME
ENTRYPOINT ["/opt/kafka/run.sh"]