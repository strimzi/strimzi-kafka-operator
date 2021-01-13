FROM strimzi/kafka:latest

ENV JAVA_OPTS "-DLOG_LEVEL=info"

# copy scripts for starting Kafka
COPY scripts $KAFKA_HOME

USER 1001
