FROM quay.io/scholzj/kafka:latest-kafka-3.6.0
USER root:root
RUN curl -LO https://repo1.maven.org/maven2/org/apache/kafka/kafka-storage/3.6.0/kafka-storage-3.6.0-test.jar; \
    mv kafka-storage-3.6.0-test.jar /opt/kafka/libs/kafka-storage-3.6.0-test.jar
USER 1001
