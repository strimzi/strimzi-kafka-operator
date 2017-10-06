FROM enmasseproject/kafka-base:latest

EXPOSE 8083

# copy scripts for starting Kafka Connect
COPY ./scripts/ $KAFKA_HOME

USER kafka:kafka

ENTRYPOINT ["/opt/kafka/kafka_connect_run.sh"]