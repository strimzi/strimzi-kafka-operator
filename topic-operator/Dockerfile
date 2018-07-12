FROM strimzi/java-base:latest

ARG strimzi_version=1.0-SNAPSHOT
ENV STRIMZI_VERSION ${strimzi_version}

ADD target/topic-operator-${strimzi_version}.jar /

CMD /bin/launch_java.sh /topic-operator-${STRIMZI_VERSION}.jar