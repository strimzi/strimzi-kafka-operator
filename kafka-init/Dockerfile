FROM strimzi/java-base:latest

ARG strimzi_version=1.0-SNAPSHOT
ENV STRIMZI_VERSION ${strimzi_version}

ADD target/kafka-init-${strimzi_version}.jar /

CMD /bin/launch_java.sh /kafka-init-${STRIMZI_VERSION}.jar