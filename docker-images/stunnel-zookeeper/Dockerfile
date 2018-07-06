FROM fedora:27

RUN yum -y install stunnel hostname procps-ng openssl && yum clean all -y 

# set Kafka home folder
ENV KAFKA_HOME=/opt/kafka

# Add kafka group / user
RUN groupadd -r -g 1001 kafka && useradd -r -m -u 1001 -g kafka kafka

# exposing Zookeeper ports
EXPOSE 2888 3888

RUN mkdir $KAFKA_HOME 

# copy scripts for starting Stunnel
COPY ./scripts/ $KAFKA_HOME

WORKDIR $KAFKA_HOME

USER kafka:kafka

#CMD ["/opt/kafka/stunnel_run.sh"]
CMD ["sh", "-c", "tail -f /dev/null"]
