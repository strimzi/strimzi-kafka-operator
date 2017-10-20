# Kafka as a Service

Barnabas provides a way to run an [Apache Kafka][kafka] cluster on 
[Kubernetes][k8s] or [OpenShift][os] in various deployment configurations.

<!-- TOC -->

- [Kafka as a Service](#kafka-as-a-service)
    - [Kafka Stateful Sets](#kafka-stateful-sets)
        - [Deploying to OpenShift](#deploying-to-openshift)
        - [Deploying to Kubernetes](#deploying-to-kubernetes)
    - [Kafka in-memory](#kafka-in-memory)
        - [Deploying to OpenShift](#deploying-to-openshift-1)
        - [Deploying to Kubernetes](#deploying-to-kubernetes-1)
    - [Kafka Connect](#kafka-connect)
        - [Deploying to OpenShift](#deploying-to-openshift-2)
        - [Deploying to Kubernetes](#deploying-to-kubernetes-2)
        - [Using Kafka Connect with additional plugins](#using-kafka-connect-with-additional-plugins)
            - [Mount a volume containing the plugins](#mount-a-volume-containing-the-plugins)
            - [Create a new image based on `enmasseproject/kafka-connect`](#create-a-new-image-based-on-enmasseprojectkafka-connect)
            - [Using Openshift Build and S2I image](#using-openshift-build-and-s2i-image)

<!-- /TOC -->

## Kafka Stateful Sets

This deployment uses the StatefulSets (previously known as "PetSets") feature of Kubernetes/OpenShift. 
With StatefulSets, the pods receive a unique name and network identity and that makes it easier to identify the 
individual Kafka broker pods and set their identity (broker ID). 
Each Kafka broker pod is using its own PersistentVolume. 
The PersistentVolume is acquired using PersistentVolumeClaim – that makes it independent on the actual type of the PersistentVolume. 
For example, it can use HostPath volumes on Minikube or Amazon EBS volumes in Amazon AWS deployments without any changes in the YAML files.

It's important to say that in this deployment both _regular_ and _headless_ services are used:

* regular services can be used as bootstrap servers for Kafka clients;
* headless services are needed to have DNS resolve the pods IP addresses directly.

This deployment is available under the _kafka-statefulsets_ folder and provides following artifacts:

* Dockerfile : Docker file for building an image with Kafka and Zookeeper already installed
* config : configuration file templates for running Zookeeper
* scripts : scripts for starting up Kafka and Zookeeper servers
* resources : provides all YAML configuration files for setting up volumes, services and deployments

### Deploying to OpenShift

1. With the [Openshift client tools][origin] installed and having logged into your cluster, 
   create the [provided "barnabas" template](kafka-statefulsets/resources/openshift-template.yaml) 
   by running

        oc create -f kafka-statefulsets/resources/openshift-template.yaml

   in your terminal. This template provides the "zookeeper" StatefulSet with a single replica, the "kafka" StatefulSet with 3 replicas,
   and the "zookeeper", "zookeeper-headless", "kafka" and "kafka-headless" Services.
2. Create a new app using the "barnabas" template:

        oc new-app barnabas

### Deploying to Kubernetes

1. If you don't have a cluster running, start one (e.g. by executing `minikube start`)
2. If your cluster doesn't have any default storage class, create the persistent volumes manually

        kubectl apply -f kafka-statefulsets/resources/cluster-volumes.yaml

3. Create the services by running:

        kubectl apply -f kafka-statefulsets/resources/zookeeper.yaml && \
        kubectl apply -f kafka-statefulsets/resources/zookeeper-headless-service.yaml && \
        kubectl apply -f kafka-statefulsets/resources/zookeeper-service.yaml && \
        kubectl apply -f kafka-statefulsets/resources/kafka.yaml && \
        kubectl apply -f kafka-statefulsets/resources/kafka-headless-service.yaml && \
        kubectl apply -f kafka-statefulsets/resources/kafka-service.yaml

4. You can then verify that the services started using

        kubectl describe all


## Kafka in-memory

Kafka in-memory deployment is just for development and testing purposes and not for production. 
It is designed the same way as the Kafka StatefulSets deployment. 
The only difference is that for storing broker information (Zookeeper side) and topics/partitions (Kafka side), an _emptyDir_ is used instead of Persistent Volume Claims. 
This means that its content is strictly related to the pod life cycle (deleted when the pod goes down). 
This makes the in-memory deployment well-suited to development and testing because you don't have to provide persistent volumes.

This deployment is available under the _kafka-inmemory_ folder and provides following artifacts :

* resources : provides all YAML configuration files for setting up services and deployments

### Deploying to OpenShift

1. With the [Openshift client tools][origin] installed and having logged into your cluster, 
   create a pod using the [provided template](kafka-inmemory/resources/openshift-template.yaml) by running

        oc create -f kafka-inmemory/resources/openshift-template.yaml

   in your terminal. This template provides the "zookeeper" and the "kafka" deployments and the 
   "zookeeper-service" and "kafka-service" services. 
2. Create a new app:

        oc new-app barnabas-inmemory


### Deploying to Kubernetes

1. If you don't have a cluster running, start one (e.g. by executing `minikube start`)
2. Create the deployments and services by running:

        kubectl apply -f kafka-inmemory/resources/zookeeper.yaml && \
        kubectl apply -f kafka-inmemory/resources/zookeeper-service.yaml && \
        kubectl apply -f kafka-inmemory/resources/zookeeper-headless-service.yaml && \
        kubectl apply -f kafka-inmemory/resources/kafka.yaml && \
        kubectl apply -f kafka-inmemory/resources/kafka-service.yaml && \
        kubectl apply -f kafka-inmemory/resources/kafka-headless-service.yaml

3. You can then verify that the services started using

        kubectl describe all


## Kafka Connect

This deployment adds a [Kafka Connect][connect] cluster which can be used with either of the Kafka deployments described above. 
It is implemented as a deployment with a configurable number of workers. 
The default image currently contains only the Connectors distributed with Apache Kafka Connect - 
`FileStreamSinkConnector` and `FileStreamSourceConnector`. 
The REST interface for managing the Kafka Connect cluster is exposed internally within the Kubernetes / OpenShift 
cluster as service `kafka-connect` on port `8083`.


### Deploying to OpenShift

1. Deploy a Kafka broker to your OpenShift cluster using either of the [in-memory](#kafka-in-memory) or 
   [statefulsets deployments](#kafka-stateful-sets) above.
2. Create a pod using the [provided template](kafka-connect/resources/openshift-template.yaml) by running

        oc create -f kafka-connect/resources/openshift-template.yaml

   in your terminal.
3. Create a new app:

        oc new-app barnabas-connect


### Deploying to Kubernetes

1. Deploy a Kafka broker to your Kubernetes cluster using either of the [in-memory](#kafka-in-memory) or 
   [statefulsets deployments](#kafka-stateful-sets) above.
2. Start the deployment by running 

        kubectl apply -f kafka-connect/resources/kafka-connect.yaml

   in your terminal.
3. Start the Connect service by running

        kubectl apply -f kafka-connect/resources/kafka-connect-service.yaml

   in your terminal.


### Using Kafka Connect with additional plugins

Our Kafka Connect images contain by default only the `FileStreamSinkConnector` and `FileStreamSourceConnector` connectors.
If you want to use other connectors, you can:

* Mount a volume containing the plugins to path `/opt/kafka/plugins/`
* Use the `enmasseproject/kafka-connect` image as Docker base image, add your connectors to the `/opt/kafka/plugins/` 
  directory and use this new image instead of `enmasseproject/kafka-connect`
* Use OpenShift build system and our S2I image

#### Mount a volume containing the plugins

1. Prepare a PersistentVolume which contains a directory with your plugin(s) and their dependencies and ensure 
   these files are world-readable (`chmod -R a+r /path/to/your/directory`).
2. Mount the volume into your Pod at the path `/opt/kafka/plugins/`

#### Create a new image based on `enmasseproject/kafka-connect`

1. Create a new `Dockerfile` which uses `enmasseproject/kafka-connect`
```Dockerfile
FROM enmasseproject/kafka-connect:latest
USER root:root
COPY ./my-plugin/ /opt/kafka/plugins/
USER kafka:kafka
```
2. Build the Docker image and upload it to your Docker repository
3. Use your new Docker image in your Kafka Connect deployment

#### Using Openshift Build and S2I image

OpenShift supports [Builds](https://docs.openshift.org/3.6/dev_guide/builds/index.html) which can be used together 
with [Source-to-Image (S2I)](https://docs.openshift.org/3.6/creating_images/s2i.html#creating-images-s2i) framework to 
create new Docker images. OpenShift Build takes a builder image with the S2I support together with source code 
and/or binaries provided by the user and uses them to build a new Docker image. 
The newly created Docker Image will be stored in OpenShift's local Docker repository and can be used in deployments. 
The Barnabas project provides a Kafka Connect S2I builder image [`enmasseproject/kafka-connect-s2i`](https://hub.docker.com/r/enmasseproject/kafka-connect-s2i/) 
which takes user-provided binaries (with plugins and connectors) and creates a new Kafka Connect image. 
This enhanced Kafka Connect image can be used with our Kafka Connect deployment.

To configure the OpenShift Build and create a new Kafka Connect image, follow these steps:

1. Create OpenShift build configuration and Kafka Connect deployment using our OpenShift template
```
oc apply -f kafka-connect/s2i/resources/openshift-template.yaml
oc new-app barnabas-connect-s2i
```
2. Prepare a directory with Kafka Connect plugins which you want to use. For example:
```
$ tree ./my-plugins/
./my-plugins/
└── kafka-connect-jdbc
    ├── kafka-connect-jdbc-3.3.0.jar
    ├── postgresql-9.4-1206-jdbc41.jar
    └── sqlite-jdbc-3.8.11.2.jar
```
3. Start new image build using the prepared directory
```
oc start-build kafka-connect --from-dir ./my-plugins/
```
4. Once the build is finished, the new image will be automatically used with your Kafka Connect deployment.

[kafka]: https://kafka.apache.org "Apache Kafka"
[connect]: https://kafka.apache.org/documentation/#connect "Apache Kafka Connect"
[k8s]: https://kubernetes.io/ "Kubernetes"
[os]: https://www.openshift.com/ "OpenShift"
[statefulset]: https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/ "Kubernetes StatefulSets"
[origin]: https://github.com/openshift/origin/releases "OpenShift Origin releases"



