FROM strimzi/kafka-base:latest

# exposing Zookeeper client port and the one for JMX exporter
EXPOSE 9404

# copy scripts for starting Zookeeper
COPY ./scripts/ $KAFKA_HOME

CMD ["/opt/kafka/zookeeper_run.sh"]
