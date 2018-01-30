FROM strimzi/kafka-base:latest

EXPOSE 2181 2888 3888 9404

# copy configuration files
COPY ./config/ $KAFKA_HOME/config/

# copy scripts for starting Kafka and Zookeeper
COPY ./scripts/ $KAFKA_HOME

USER kafka:kafka

CMD ["/opt/kafka/zookeeper_run.sh"]