FROM strimzi/java-base:8-3

ARG strimzi_version=1.0-SNAPSHOT
ENV STRIMZI_VERSION ${strimzi_version}

ADD target/topic-controller-${STRIMZI_VERSION}.jar /

CMD ["/bin/launch_java.sh", "/topic-controller-${STRIMZI_VERSION}.jar"]