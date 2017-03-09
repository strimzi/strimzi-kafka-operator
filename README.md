# Kafka as a Service

This project provides a way to run a Kafka cluster on Kubernetes and OpenShift with following artifacts :

* Dockerfile : Docker file for building an image with Kafka and Zookeeper already installed
* config : configuration file templates for running Kafka and Zookeeper
* scripts : scripts for starting up Kafka and Zookeeper servers
* resources : provides all YAML configuration files for setting up services, deployments, volumes and claims
* kubernetes : contains scripts for provisioning and deprovisiong stuff on Kubernetes
* openshift : contains scripts for provisioning and deprovisiong stuff on OpenShift
