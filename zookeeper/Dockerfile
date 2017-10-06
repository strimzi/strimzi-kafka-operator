FROM fedora:26

RUN dnf -y install java-1.8.0-openjdk gettext hostname && dnf clean all -y

# set Kafka home folder
ENV ZOOKEEPER_HOME=/opt/zookeeper

# Add zookeeper group / user
RUN groupadd -r zookeeper && useradd -r -m -g zookeeper zookeeper

# Set Zookeeper version
ENV ZOOKEEPER_VERSION=3.4.10

# Set MD5checksum
ENV CHECKSUM="e4cf1b1593ca870bf1c7a75188f09678  zookeeper-3.4.10.tar.gz"

# Set from build args
ARG version=latest
ENV VERSION ${version}

WORKDIR $ZOOKEEPER_HOME

# downloading/extracting Apache Zookeeper
RUN curl -O http://www.eu.apache.org/dist/zookeeper/zookeeper-$ZOOKEEPER_VERSION/zookeeper-$ZOOKEEPER_VERSION.tar.gz  \
    && echo $CHECKSUM > zookeeper-$ZOOKEEPER_VERSION.tar.gz.md5                                     \
    && md5sum --check zookeeper-$ZOOKEEPER_VERSION.tar.gz.md5                                       \
    && tar xvfz zookeeper-$ZOOKEEPER_VERSION.tar.gz -C $ZOOKEEPER_HOME --strip-components=1             \
    && rm -f zookeeper-$ZOOKEEPER_VERSION.tar.gz*

EXPOSE 2181

# copy template configuration files
COPY ./config/zookeeper.properties.template $ZOOKEEPER_HOME/config/
# copy scripts for starting Kafka and Zookeeper
COPY ./scripts/zookeeper_run.sh $ZOOKEEPER_HOME

USER zookeeper:zookeeper

ENTRYPOINT ["/opt/zookeeper/zookeeper_run.sh"]