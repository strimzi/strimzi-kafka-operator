# Kafka as a Service

This project provides a way to run an Apache Kafka cluster on Kubernetes and OpenShift with different types of deployment.

## Kafka persisted

With this deployment, a persistent volume is used in order to persist data for both Zookeeper server and Kafka brokers. At same time, with this deployment, when a Kafka instance
goes down, the next one will replace the previous with same broker-id in order to avoid a lot of messages traffic from the other broker instances for synchronizing all topic partitions. In this way, the new instance (which replace the previous one crashed) needs to fetch only the messages stored on the other brokers during the down time.

This deployment is available under the _kafka-persisted_ folder and provides following artifacts :

* Dockerfile : Docker file for building an image with Kafka and Zookeeper already installed
* config : configuration file templates for running Kafka and Zookeeper
* scripts : scripts for starting up Kafka and Zookeeper servers
* resources : provides all YAML configuration files for setting up services, deployments, volumes and claims

## Kafka in-memory

This deployment is just for development and testing purpose and not for production. The Zookeeper server and the Kafka broker are deployed in different pods. For storing broker information (Zookeeper side) and topics/partitions (Kafka side), an _emptyDir_ is used so it means that its content is strictly related to the pod life cycle (deleted when the pod goes down). Finally, this deployment is not for scaling but just for having one single pod for Kafka broker and one for Zookeeper server.

This deployment is available under the _kafka-inmemory_ folder and provides following artifacts :

* Dockerfile : Docker file for building an image with Kafka and Zookeeper already installed
* config : configuration file templates for running Zookeeper
* scripts : scripts for starting up Kafka and Zookeeper servers
* resources : provides all YAML configuration files for setting up services and deployments
