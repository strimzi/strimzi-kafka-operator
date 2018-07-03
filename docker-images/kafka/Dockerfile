FROM strimzi/kafka-base:latest

# exposing Kafka ports and the one for JMX exporter
EXPOSE 9091 9092 9404

# copy scripts for starting Kafka
COPY ./scripts/ $KAFKA_HOME

USER kafka:kafka

CMD ["/opt/kafka/kafka_run.sh"]