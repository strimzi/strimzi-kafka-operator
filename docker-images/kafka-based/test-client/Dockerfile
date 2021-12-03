FROM strimzi/kafka:latest

ENV JAVA_OPTS "-DLOG_LEVEL=info"

USER root

# copy scripts for starting Kafka
COPY scripts $KAFKA_HOME

RUN mkdir $KAFKA_HOME/jmxterm;\
    curl -L https://github.com/jiaqi/jmxterm/releases/download/v1.0.2/jmxterm-1.0.2-uber.jar -o $KAFKA_HOME/jmxterm/jmxterm.jar;

USER 1001
