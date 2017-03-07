# Kafka as a Service

This project provides a way to run a Kafka cluster on Kubernetes and OpenShift with following artifacts :

* docker : contains a Dockerfile for building an image with Kafka and Zookeeper already installed and scripts for starting up the servers on it
* cluster : provides all YAML configuration files for setting up Kubernetes services, replication controllers, volumes and claims
* kubernetes : contains scripts for provisioning and deprovisiong stuff on Kubernetes
* openshift : contains scripts for provisioning and deprovisiong stuff on OpenShift
