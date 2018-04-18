FROM strimzi/kafka-base:latest

# exposing Zookeeper ports and the one for JMX exporter
EXPOSE 2181 2888 3888 9404

# copy configuration files
COPY ./config/ $KAFKA_HOME/config/

# copy scripts for starting Zookeeper
COPY ./scripts/ $KAFKA_HOME

USER kafka:kafka

CMD ["/opt/kafka/zookeeper_run.sh"]