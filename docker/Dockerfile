FROM java:8u72

MAINTAINER Paolo Patierno

# install gettext for envsubst
RUN apt-get update
RUN apt-get install -y gettext

# install tcpdump for protocol level debugging
RUN apt-get install -y tcpdump
# install vim for text editing
RUN apt-get install -y vim

# set Scala and Kafka version
ENV SCALA_VERSION=2.11
ENV KAFKA_VERSION=0.9.0.1

# downloading/extracting Apache Kafka
RUN wget http://www.eu.apache.org/dist/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz

RUN tar xvfz kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz -C /opt

# set Kakfa home folder
ENV KAFKA_HOME=/opt/kafka_$SCALA_VERSION-$KAFKA_VERSION

# copy template configuration files
COPY ./config/ $KAFKA_HOME/config
# copy scripts for starting Kafka
COPY ./kafka_run.sh $KAFKA_HOME
RUN chmod +x $KAFKA_HOME/kafka_run.sh
COPY ./kafka_pre_run.py $KAFKA_HOME
RUN chmod +x $KAFKA_HOME/kafka_pre_run.py
