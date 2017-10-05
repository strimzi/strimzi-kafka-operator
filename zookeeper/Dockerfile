FROM scholzj/kafka-base:latest

EXPOSE 2181

# copy template configuration files
COPY ./config/zookeeper.properties.template $KAFKA_HOME/config/
# copy scripts for starting Kafka and Zookeeper
COPY ./scripts/zookeeper_run.sh $KAFKA_HOME

USER root
RUN chown kafka:kafka zookeeper_run.sh \
    && chown kafka:kafka config/zookeeper.properties.template
USER kafka

ENTRYPOINT ["/opt/kafka/zookeeper_run.sh"]