FROM strimzi/java-base:latest

ARG strimzi_version=1.0-SNAPSHOT
ENV STRIMZI_VERSION ${strimzi_version}

COPY ./scripts/ /bin

COPY target/user-operator-${strimzi_version}.jar /

USER 1001

CMD /bin/user_operator_run.sh /user-operator-${STRIMZI_VERSION}.jar
