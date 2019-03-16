FROM strimzi/kafka-base:latest

ARG strimzi_version=1.0-SNAPSHOT
ENV STRIMZI_VERSION ${strimzi_version}
ENV JAVA_OPTS "-DLOG_LEVEL=info"

# copy scripts for starting Kafka
COPY scripts $KAFKA_HOME
COPY scripts/dynamic_resources.sh /bin/dynamic_resources.sh

ADD target/test-client-${STRIMZI_VERSION}.jar /test-client.jar

RUN chmod 777 /test-client.jar

USER 1001

EXPOSE 4242/tcp

CMD ["/opt/kafka/launch_java.sh", "/test-client.jar"]