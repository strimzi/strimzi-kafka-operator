asciidoctor -t -dbook \
    -a ProductLongName="Red Hat AMQ Streams on OpenShift" \
    -a ProductVersion="1.0.0 Development Preview 1" \
    -a ProductName="AMQ Streams on OpenShift" \
    -a ProductPlatformName=OpenShift \
    -a ProductPlatformLongName="OpenShift Container Platform" \
    -a DockerRepository="https://access.redhat.com/containers/[Red Hat Container Catalog]" \
    -a DockerZookeeper="jboss-amqstreams-1-tech-preview/amqstreams10-zookeeper-openshift:1.0" \
    -a DockerKafka="jboss-amqstreams-1-tech-preview/amqstreams10-kafka-openshift:1.0" \
    -a DockerKafkaConnect="jboss-amqstreams-1-tech-preview/amqstreams10-kafkaconnect-openshift:1.0" \
    -a DockerKafkaConnectS2I="jboss-amqstreams-1-tech-preview/amqstreams10-kafkaconnects2i-openshift:1.0" \
    -a DockerTopicController="jboss-amqstreams-1-tech-preview/amqstreams10-topiccontroller-openshift:1.0" \
    -a Kubernetes! \
    -a InstallationAppendix! \
    -a MetricsAppendix! \
    -o html/book.html \
    documentation/book/master.adoc