FROM strimzi/base:latest

ENV JMXTRANS_HOME /usr/share/jmxtrans
ENV PATH $JMXTRANS_HOME/bin:$PATH
ENV JAR_FILE $JMXTRANS_HOME/lib/jmxtrans.jar
ENV JMXTRANS_VERSION 271
ENV JMXTRANS_CHECKSUM="9c9116b628be912a723fae8ab65853908cc0872139340827b4af3dbdb7274bc88956af7f33dc927a43ea291c721701fb52368ccd414218ef33e7de3060baf849  jmxtrans-${JMXTRANS_VERSION}-all.jar"
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
