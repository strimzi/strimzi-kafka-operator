FROM strimzi/java-base:latest

ARG strimzi_version=1.0-SNAPSHOT
ENV STRIMZI_VERSION ${strimzi_version}

COPY target/cluster-operator-${strimzi_version}.jar /

USER 1001

CMD /bin/launch_java.sh /cluster-operator-${STRIMZI_VERSION}.jar
