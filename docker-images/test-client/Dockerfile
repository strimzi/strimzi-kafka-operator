FROM strimzi/kafka:latest

ENV JAVA_OPTS "-DLOG_LEVEL=info"

# copy scripts for starting Kafka
COPY scripts $KAFKA_HOME
COPY scripts/dynamic_resources.sh /bin/dynamic_resources.sh

USER 1001