FROM strimzi/kafka-base:latest

EXPOSE 8083 9404

# copy configs for Kafka Connect
COPY ./config/ $KAFKA_HOME/config/

# copy scripts for starting Kafka Connect
COPY ./scripts/ $KAFKA_HOME

USER kafka:kafka

CMD ["/opt/kafka/kafka_connect_run.sh"]
