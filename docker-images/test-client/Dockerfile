FROM strimzi/kafka:latest

ARG strimzi_version=1.0-SNAPSHOT
ENV STRIMZI_VERSION ${strimzi_version}

# copy scripts for starting Kafka
COPY ./scripts/ $KAFKA_HOME
ADD test-client/target/test-client-${VERSION}.jar /test-client.jar

USER 1001

