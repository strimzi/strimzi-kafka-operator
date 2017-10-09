FROM fedora:26

RUN dnf -y install java-1.8.0-openjdk gettext hostname && dnf clean all -y

# set Kafka home folder
ENV ZOOKEEPER_HOME=/opt/zookeeper

# Add zookeeper group / user
RUN groupadd -r -g 1001 zookeeper && useradd -r -m -u 1001 -g zookeeper zookeeper

# Set Zookeeper version
ENV ZOOKEEPER_VERSION=3.4.10

# Set MD5checksum
ENV CHECKSUM="eb2145498c5f7a0d23650d3e0102318363206fba  zookeeper-3.4.10.tar.gz"

# Set from build args
ARG version=latest
ENV VERSION ${version}

WORKDIR $ZOOKEEPER_HOME

# downloading/extracting Apache Zookeeper
RUN curl -O https://www.apache.org/dist/zookeeper/zookeeper-$ZOOKEEPER_VERSION/zookeeper-$ZOOKEEPER_VERSION.tar.gz  \
    && echo $CHECKSUM > zookeeper-$ZOOKEEPER_VERSION.tar.gz.md5                                     \
    && sha1sum --check zookeeper-$ZOOKEEPER_VERSION.tar.gz.md5                                       \
    && tar xvfz zookeeper-$ZOOKEEPER_VERSION.tar.gz -C $ZOOKEEPER_HOME --strip-components=1             \
    && rm -f zookeeper-$ZOOKEEPER_VERSION.tar.gz*

EXPOSE 2181 2888 3888

# copy template configuration files
COPY ./config/zookeeper.properties.template $ZOOKEEPER_HOME/config/
# copy scripts for starting Kafka and Zookeeper
COPY ./scripts/zookeeper_run.sh $ZOOKEEPER_HOME

USER zookeeper:zookeeper

ENTRYPOINT ["/opt/zookeeper/zookeeper_run.sh"]