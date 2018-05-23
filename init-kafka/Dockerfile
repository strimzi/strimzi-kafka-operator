FROM strimzi/java-base:8-3

ARG strimzi_version=1.0-SNAPSHOT
ENV STRIMZI_VERSION ${strimzi_version}

ADD target/init-kafka-${strimzi_version}.jar /

CMD /bin/launch_java.sh /init-kafka-${STRIMZI_VERSION}.jar