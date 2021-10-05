FROM strimzi/base:latest

ENV JMXTRANS_HOME /usr/share/jmxtrans
ENV PATH $JMXTRANS_HOME/bin:$PATH
ENV JAR_FILE $JMXTRANS_HOME/lib/jmxtrans.jar
ENV JMXTRANS_VERSION 272
ENV JMXTRANS_CHECKSUM="834eef1957e553ad561cd0730f985570e68e0771be743172d371e89a3c6eeeb3d6dfee9a79b08987a4c0e269dd121ff96d24eeef39ff6982a6474f0675825dc6  jmxtrans-${JMXTRANS_VERSION}-all.jar"
ENV HEAP_SIZE 512
ENV PERM_SIZE 384
ENV MAX_PERM_SIZE 384
ENV SECONDS_BETWEEN_RUNS 60
ENV CONTINUE_ON_ERROR false
ENV JSON_DIR /var/lib/jmxtrans

WORKDIR ${JMXTRANS_HOME}
RUN mkdir -p ${JMXTRANS_HOME}/conf

#####
# Add JmxTrans Jar
#####
RUN mkdir -p /usr/share/jmxtrans/lib/ \
    && mkdir -p /var/log/jmxtrans \
    && curl -k https://repo1.maven.org/maven2/org/jmxtrans/jmxtrans/${JMXTRANS_VERSION}/jmxtrans-${JMXTRANS_VERSION}-all.jar --output jmxtrans-${JMXTRANS_VERSION}-all.jar \
    && echo $JMXTRANS_CHECKSUM > jmxtrans-${JMXTRANS_VERSION}-all.jar.sha512 \
    && sha512sum --check jmxtrans-${JMXTRANS_VERSION}-all.jar.sha512 \
    && rm jmxtrans-${JMXTRANS_VERSION}-all.jar.sha512 \
    && mv jmxtrans-${JMXTRANS_VERSION}-all.jar ${JAR_FILE}

COPY docker-entrypoint.sh /docker-entrypoint.sh
COPY jmxtrans_readiness_check.sh /opt/jmx/

#####
# Add NC
#####
RUN microdnf install nc \
    && microdnf clean all

VOLUME ${JSON_DIR}

ENTRYPOINT [ "/docker-entrypoint.sh",  "start-without-jmx" ]

USER 1001
