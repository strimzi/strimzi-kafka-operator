FROM strimzi/base:latest

ARG strimzi_version=1.0-SNAPSHOT
ENV STRIMZI_VERSION ${strimzi_version}
ENV STRIMZI_HOME=/opt/strimzi
RUN mkdir -p ${STRIMZI_HOME}/lib
WORKDIR ${STRIMZI_HOME}

COPY tmp/crd-convert-${strimzi_version}.jar ${STRIMZI_HOME}/lib

CMD java -jar lib/crd-convert-${STRIMZI_VERSION}.jar