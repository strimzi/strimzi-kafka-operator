# Kafka as a Service

This project provides a way to run an Apache Kafka cluster on Kubernetes and OpenShift with different types of deployment.

<!-- TOC -->

- [Kafka as a Service](#kafka-as-a-service)
    - [Kafka Stateful Sets](#kafka-stateful-sets)
        - [Deploying to OpenShift](#deploying-to-openshift)
    - [Kafka in-memory](#kafka-in-memory)
        - [Deploying to OpenShift](#deploying-to-openshift-1)
    - [Kafka Connect](#kafka-connect)
        - [Deploying to OpenShift](#deploying-to-openshift-2)
        - [Deploying to Kubernetes](#deploying-to-kubernetes)
        - [Using Kafka Connect with additional plugins](#using-kafka-connect-with-additional-plugins)
            - [Mount a volume containing the plugins](#mount-a-volume-containing-the-plugins)
            - [Create a new image based on `enmasseproject/kafka-connect`](#create-a-new-image-based-on-enmasseprojectkafka-connect)
            - [Using Openshift Build and S2I image](#using-openshift-build-and-s2i-image)

<!-- /TOC -->

## Kafka Stateful Sets

This deployment uses the Stateful Sets (previously known as "Pet Sets") feature of Kubernetes / OpenShift. With Stateful Sets, the pods receive unique name and network identity and that makes it easier to identify the individual Kafka broker pods and set their identity (broker ID). Each Kafka broker pod is using its own Persistent Volume. The Persistent Volume is acquired using Persistent Volume Claim - that makes it independent on the actual type of the Persistent Volume. It can use HostPath volumes on Minikube or Amazon EBS disks in public cloud deployments.

It's important to say that in this deployment both "regular" and "headless" services are used. The "regular" services are needed for having instances accessible from clients;
the "headless" services are needed for having the DNS resolving directly the PODs IP addresses for having the Stateful Sets working properly.

This deployment is available under the _kafka-statefulsets_ folder and provides following artifacts :

* Dockerfile : Docker file for building an image with Kafka and Zookeeper already installed
* config : configuration file templates for running Zookeeper
* scripts : scripts for starting up Kafka and Zookeeper servers
* resources : provides all YAML configuration files for setting up volumes, services and deployments

### Deploying to OpenShift

To conveniently deploy a StatefulSet to OpenShift a [Template is provided](kafka-statefulsets/resources/openshift-template.yaml), this could be used via `oc create -f kafka-statefulsets/resources/openshift-template.yaml` so it is present within your project. To create a whole Kafka StatefulSet use `oc new-app barnabas`.

## Kafka in-memory

Kafka in-memory deployment is just for development and testing purpose and not for production. It is designed the same way as the Kafka Stateful Sets deployment. The only difference is that for storing broker information (Zookeeper side) and topics/partitions (Kafka side), an _emptyDir_ is used instead of Persistent Volume Claims. This means that its content is strictly related to the pod life cycle (deleted when the pod goes down). However for development and testing it might be easier to use the in-memory deployment because it doesn't need to provision the persistent volumes needed for the stateful set deployment.

This deployment is available under the _kafka-inmemory_ folder and provides following artifacts :

* resources : provides all YAML configuration files for setting up services and deployments

### Deploying to OpenShift

To conveniently deploy a StatefulSet to OpenShift a [Template is provided](kafka-inmemory/resources/openshift-template.yaml), this could be used via `oc create -f kafka-inmemory/resources/openshift-template.yaml` so it is present within your project. To create a whole Kafka in-memory cluster use `oc new-app barnabas`.

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

#### Using Openshift Build and S2I image

OpenShift supports [Builds](https://docs.openshift.org/3.6/dev_guide/builds/index.html) which can be used together with [Source-to-Image (S2I)](https://docs.openshift.org/3.6/creating_images/s2i.html#creating-images-s2i) framework to create new Docker images. OpenShift Build takes a builder image with the S2I support and source codes or binaries provided by the user and will use them to build a new Docker image. The newly created docker Image will be stored in OpenShift's local Docker repository and can be used in deployments. Barnabas project provides Kafka Connect S2I builder image [`enmasseproject/kafka-connect-s2i`](https://hub.docker.com/r/enmasseproject/kafka-connect-s2i/) which takes user provided binaries (with plugins and connectors) and creates a new Kafka Connect image. This enhanced Kafka Connect image can be used with our [Kafka Connect deployment](#kafka-connect).

To configure the OpenShift Build and create new Kafka Connect image, follow these steps:

* Create OpenShift build configuration using our OpenShift template
```
oc apply -f kafka-connect/s2i/resources/openshift-template.yaml
oc new-app barnabas-connect-s2i
```
* Prepare a directory with Kafka Connect plugins which you want to use. For example:
```
$ tree ./my-plugins/
./my-plugins/
└── kafka-connect-jdbc
    ├── kafka-connect-jdbc-3.3.0.jar
    ├── postgresql-9.4-1206-jdbc41.jar
    └── sqlite-jdbc-3.8.11.2.jar
```
* Start new image build using the prepared directory
```
oc start-build kafka-connect --from-dir ./my-plugins/
```
* You can find out the address of your local Docker repository (for example using `oc get imagestreams kafka-connect`)
* Use your new image in the Kafka Connect deployment
```
oc create -f kafka-connect/resources/openshift-template.yaml
oc new-app -p IMAGE_REPO_NAME=$(oc get is kafka-connect -o=jsonpath={.status.dockerImageRepository} | sed 's/\(.*\)\/.*/\1/') barnabas-connect
```