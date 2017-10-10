# Kafka as a Service

This project provides a way to run an Apache Kafka cluster on Kubernetes and OpenShift with different types of deployment.

<!-- TOC -->

- [Kafka as a Service](#kafka-as-a-service)
    - [Kafka in-memory](#kafka-in-memory)
    - [Kafka Stateful Sets](#kafka-stateful-sets)
        - [Deploying to OpenShift](#deploying-to-openshift)
    - [Kafka Connect](#kafka-connect)
        - [Deploying to OpenShift](#deploying-to-openshift-1)
        - [Deploying to Kubernetes](#deploying-to-kubernetes)
        - [Using Kafka Connect with additional plugins](#using-kafka-connect-with-additional-plugins)
            - [Mount a volume containing the plugins](#mount-a-volume-containing-the-plugins)
            - [Create a new image based on `enmasseproject/kafka-connect`](#create-a-new-image-based-on-enmasseprojectkafka-connect)
            - [Using Openshift build and S2I image](#using-openshift-build-and-s2i-image)

<!-- /TOC -->

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

### Deploying to OpenShift

To conveniently deploy Kafka Connect to OpenShift a [Template is provided](kafka-connect/resources/openshift-template.yaml), this could be used via `oc create -f kafka-connect/resources/openshift-template.yaml` so it is present within your project. To create a whole Kafka Connect cluster use `oc new-app barnabas-connect` *(make sure you have already deployed a Kafka broker)*.

### Deploying to Kubernetes

To conveniently deploy Kafka Connect to Kubernetes use the provided yaml files with the Kafka Connect [deployment](kafka-connect/resources/kafka-connect.yaml) and [service](kafka-connect/resources/kafka-connect-service.yaml). This could be done using `kubectl apply -f kafka-connect/resources/kafka-connect.yaml` and `kubectl apply -f kafka-connect/resources/kafka-connect-service.yaml`.

### Using Kafka Connect with additional plugins

Our Kafka Connect images contain by default only the `FileStreamSinkConnector` and `FileStreamSourceConnector` connectors.
If you want to use other connectors, you can:
* Mount a volume containing the plugins to path `/opt/kafka/plugins/`
* Use the `enmasseproject/kafka-connect` image as Docker base image, add your connectors to the `/opt/kafka/plugins/` directory and use this new image instead of `enmasseproject/kafka-connect`
* Use OpenShift build system and our S2I image

#### Mount a volume containing the plugins

* Prepare a persistent volume which contains a directory with your plugin(s).
* Mount the volume into your Pod

#### Create a new image based on `enmasseproject/kafka-connect`

* Create new `Dockerfile` which uses `enmasseproject/kafka-connect`
```Dockerfile
FROM enmasseproject/kafka-connect:latest
USER root:root
COPY ./my-plugin/ /opt/kafka/plugins/
USER kafka:kafka
```
* Build the Docker image and upload it to your Docker repository of choice
* Use your new Docker image in your Kafka Connect deployment

#### Using Openshift build and S2I image

* Create OpenShift build configuration using our OpenShift template
```
oc apply -f kafka-connect/s2i/resources/openshift-template.yaml
oc new-app barnabas-connect-s2i
```
* Prepare a directory with Kafka Connect plugins which you want to use
* Start new image build using the prepared directory
```
oc start-build kafka-connect --from-dir ./my-plugins/
```
* Find out the address of your local Docker image repository (for example from the build logs `oc logs -f bc/kafka-connect`)
* Use your new image in the Kafka Connect deployment
```
oc create -f kafka-connect/resources/openshift-template.yaml
oc new-app -p IMAGE_REPO_NAME=172.30.1.1:5000/myproject barnabas-connect
```





