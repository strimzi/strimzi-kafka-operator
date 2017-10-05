FROM scholzj/kafka-base:latest

EXPOSE 8083

# copy scripts for starting Kafka Connect
COPY ./scripts/ $KAFKA_HOME

ENTRYPOINT ["/opt/kafka/kafka_connect_run.sh"]