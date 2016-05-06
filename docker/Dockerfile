FROM java:8u72

MAINTAINER Paolo Patierno

# install gettext for envsubst
RUN apt-get update
RUN apt-get install -y gettext

# set Scala and Kafka version
ENV SCALA_VERSION=2.11
ENV KAFKA_VERSION=0.9.0.1

# downloading/extracting Apache Kafka
RUN wget http://www.eu.apache.org/dist/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz

RUN tar xvfz kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz -C /opt

# copy template configuration files
COPY ./config/ /opt/kafka_$SCALA_VERSION-$KAFKA_VERSION/config
COPY ./kafka_run.sh /opt/kafka_$SCALA_VERSION-$KAFKA_VERSION
RUN chmod +x /opt/kafka_$SCALA_VERSION-$KAFKA_VERSION/kafka_run.sh