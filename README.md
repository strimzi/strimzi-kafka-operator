# Kafka as a Service

This project provides a way to run an Apache Kafka cluster on Kubernetes and OpenShift with different types of deployment.

<!-- TOC -->

- [Kafka as a Service](#kafka-as-a-service)
    - [Kafka persisted](#kafka-persisted)
    - [Kafka in-memory](#kafka-in-memory)
    - [Kafka Stateful Sets](#kafka-stateful-sets)
        - [Deploying to OpenShift](#deploying-to-openshift)
    - [Kafka Connect](#kafka-connect)
        - [Deploying to OpenShift](#deploying-to-openshift-1)
        - [Deploying to Kubernetes](#deploying-to-kubernetes)

<!-- /TOC -->

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

## Kafka Stateful Sets

This deployment is an improvement of the first one (the "Kafka persisted") but it uses the new Kubernetes/OpenShift feature : the Stateful Sets (previously known as "Pet Sets").
In this way, the pods receive an unique name and network identity and there is no need for a script (like the "persisted" version) where the Kafka brokers have to use a common
persisten volume for finding a free ID to get an use for the starting instance. At same time they don't need to use a single persisten volume and store logs in different folders
(named on the ID base) but they use different persistent volumes and the auto-generated claims.

It's important to say that in this deployment both "regular" and "headless" services are used. The "regular" services are needed for having instances accessible from clients;
the "headless" services are needed for having the DNS resolving directly the PODs IP addresses for having the Stateful Sets working properly.

This deployment is available under the _kafka-statefulsets_ folder and provides following artifacts :

* Dockerfile : Docker file for building an image with Kafka and Zookeeper already installed
* config : configuration file templates for running Zookeeper
* scripts : scripts for starting up Kafka and Zookeeper servers
* resources : provides all YAML configuration files for setting up volumes, services and deployments

### Deploying to OpenShift

To conveniently deploy a StatefulSet to OpenShift a [Template is provided](kafka-statefulsets/resources/openshift-template.yaml), this could be used via `oc create -f kafka-statefulsets/resources/openshift-template.yaml` so it is present within your project. To create a whole Kafka StatefulSet use `oc new-app barnabas`.

## Kafka Connect

This deployment adds Kafka Connect cluster which can be used with the Kafka deployments above. It is implemented as deployment with a configurable number of workers. The default image currently contains only the Connectors which are part of Apache Kafka project - `FileStreamSinkConnector` and `FileStreamSourceConnector`. The REST interface for managing the Kafka Connect cluster is exposed internally within the Kubernetes / OpenShift cluster as service `kafka-connect` on port `8083`.

If you want to use other connectors, you can:
* Mount a volume containing the plugins to path `/opt/kafka/plugins/`
* Use the `enmasseproject/kafka-connect` image as Docker base image, add your connectors to the `/opt/kafka/plugins/` directory and use this new image instead of `enmasseproject/kafka-connect`

### Deploying to OpenShift

To conveniently deploy Kafka Connect to OpenShift a [Template is provided](kafka-connect/resources/openshift-template.yaml), this could be used via `oc create -f kafka-connect/resources/openshift-template.yaml` so it is present within your project. To create a whole Kafka StatefulSet use `oc new-app barnabas-connect`.

### Deploying to Kubernetes

To conveniently deploy Kafka Connect to Kubernetes use the provided yaml files with the Kafka Connect [deployment](kafka-connect/resources/kafka-connect.yaml) and [service](kafka-connect/resources/kafka-connect-service.yaml). This could be done using `kubectl apply -f kafka-connect/resources/kafka-connect.yaml` and `kubectl apply -f kafka-connect/resources/kafka-connect-service.yaml`.