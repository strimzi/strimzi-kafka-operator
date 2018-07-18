FROM strimzi/java-base:latest

ARG strimzi_version=1.0-SNAPSHOT
ENV STRIMZI_VERSION ${strimzi_version}

COPY ./scripts/ /bin

ADD target/topic-operator-${strimzi_version}.jar /

CMD /bin/topic_operator_run.sh /topic-operator-${STRIMZI_VERSION}.jar