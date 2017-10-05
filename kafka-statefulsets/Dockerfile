FROM scholzj/kafka-base:latest

EXPOSE 9092

# copy scripts for starting Kafka and Zookeeper
COPY ./scripts/ $KAFKA_HOME

ENTRYPOINT ["/opt/kafka/kafka_run.sh"]