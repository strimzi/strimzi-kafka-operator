FROM strimzi/java-base:8-3

ARG strimzi_version=1.0-SNAPSHOT
ENV STRIMZI_VERSION ${strimzi_version}

ADD target/cluster-controller-${strimzi_version}.jar /

CMD /bin/launch_java.sh /cluster-controller-${STRIMZI_VERSION}.jar