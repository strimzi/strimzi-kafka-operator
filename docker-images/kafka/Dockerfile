FROM strimzi/kafka-base:latest

EXPOSE 9091 9092

# copy configuration files
COPY ./config/ $KAFKA_HOME/config/

# copy scripts for starting Kafka and Zookeeper
COPY ./scripts/ $KAFKA_HOME

USER kafka:kafka

CMD ["/opt/kafka/kafka_run.sh"]