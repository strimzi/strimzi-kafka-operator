FROM strimzi/kafka-base:latest

EXPOSE 8083 9404

# copy scripts for starting Kafka Connect
COPY ./scripts/ $KAFKA_HOME

CMD ["/opt/kafka/kafka_connect_run.sh"]
