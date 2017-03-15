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
* kubernetes : contains scripts for provisioning and deprovisiong stuff on Kubernetes
* openshift : contains scripts for provisioning and deprovisiong stuff on OpenShift
