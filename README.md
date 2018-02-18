# Strimzi : Kafka as a Service

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Build Status](https://travis-ci.org/strimzi/strimzi.svg?branch=master)](https://travis-ci.org/strimzi/strimzi)

Strimzi provides a way to run an [Apache Kafka][kafka] cluster on 
[Kubernetes][k8s] or [OpenShift][os] in various deployment configurations.

<!-- TOC depthFrom:2 -->

- [Deploying a Kubernetes/OpenShift cluster](#deploying-a-kubernetesopenshift-cluster)
    - [Kubernetes](#kubernetes)
    - [OpenShift](#openshift)
- [Cluster Controller](#cluster-controller)
    - [Deploying to OpenShift](#deploying-to-openshift)
    - [Deploying to Kubernetes](#deploying-to-kubernetes)
- [Kafka broker](#kafka-broker)
    - [Deploying to OpenShift](#deploying-to-openshift-1)
    - [Deploying to Kubernetes](#deploying-to-kubernetes-1)
- [Kafka Connect](#kafka-connect)
    - [Deploying to OpenShift](#deploying-to-openshift-2)
    - [Deploying to Kubernetes](#deploying-to-kubernetes-2)
    - [Using Kafka Connect with additional plugins](#using-kafka-connect-with-additional-plugins)
        - [Create a new image based on `strimzi/kafka-connect`](#create-a-new-image-based-on-strimzikafka-connect)
        - [Using OpenShift Build and S2I image](#using-openshift-build-and-s2i-image)
- [Metrics](#metrics)

<!-- /TOC -->

## Deploying a Kubernetes/OpenShift cluster

You will need a Kubernetes or OpenShift cluster to deploy Strimzi.

### Kubernetes

In order to interact with a Kubernetes cluster, be sure to have [the kubectl tool][kubectl] installed.
If you don't have a Kubernetes cluster up and running, the easiest way to deploy one, for development purposes,
is to use the [Minikube][minikube] project which provides a single-node cluster in a VM. In order to do that you
can just follow the installation guide which describes all the prerequisites and how to get the Minikube binaries.
Finally, the cluster can be deployed by running

```shell
minikube start
```

### OpenShift

In order to interact with an OpenShift cluster, be sure to have the [OpenShift client tools][origin] installed.
If you don't have an OpenShift cluster up and running, the easiest way to deploy one, for development purposes, is
to use the [Minishift][minishift] project which provides a single-node cluster in a VM. In order to do that you can
just follow the installation guide which describes all the prerequisites and how to get the Minishift binaries.
Finally, the cluster can be deployed by running

```shell
minishift start
```

Another way is to use the OpenShift client tools directly to spin-up a single-node cluster. It will run OpenShift as a
Docker container on the local machine.

```shell
oc cluster up
```

More information about this way can be found [here][occlusterup].

## Cluster Controller

Strimzi is using process called Cluster Controller to deploy and manage Kafka (including Zookeeper) and Kafka Connect clusters.
Cluster Controller is a process which is running inside your Kubernetes or OpenShift cluster. To deploy a Kafka cluster,
a ConfigMap with the cluster configuration has to be created. The ConfigMap needs to be labeled with following labels:

```yaml
strimzi.io/type: kafka
strimzi.io/kind: cluster
```

and contain the cluster configuration in specific format.

### Deploying to OpenShift

To deploy the Cluster Controller on OpenShift, run the following command in your terminal:

```shell
oc create -f resources/openshift/cluster-controller-with-template.yaml
```

You should be able to verify that the controller is running using:

```shell
oc describe all
```

or using the OpenShift console.

### Deploying to Kubernetes

To deploy the Cluster Controller on Kubernetes, run the following command in your terminal:

```shell
kubectl create -f resources/kubernetes/cluster-controller.yaml
```

You should be able to verify that the controller is running using:

```shell
kubectl describe all
```

## Kafka broker

Strimzi uses StatefulSets (previously known as *PetSets*) feature to deploy Kafka brokers on Kubernetes/OpenShift.
With StatefulSets, the pods receive a unique name and network identity and that makes it easier to identify the
individual Kafka broker pods and set their identity (broker ID). The deployment uses both _regular_ and _headless_
services:

- regular services can be used as bootstrap servers for Kafka clients;
- headless services are needed to have DNS resolve the pods IP addresses directly.

Together with Kafka, Strimzi will also install a Zookeeper cluster and configure the Kafka brokers to connect to it. Also
the Zookeeper cluster is using StatefulSets.

Strimzi provides two flavors of Kafka broker deployment: **ephemeral** and **persistent**. 

The **ephemeral** flavour is suitable only for development and testing purposes and not for production. The
ephemeral flavour is using `emptyDir` volumes for storing broker information (Zookeeper side) and topics/partitions
(Kafka side). Using `emptyDir` volume means that its content is strictly related to the pod life cycle (it is
deleted when the pod goes down). This makes the in-memory deployment well-suited to development and testing because
you don't have to provide persistent volumes.

The **persistent** flavour is using PersistentVolumes to store Zookeeper and Kafka data. The PersistentVolume is
acquired using PersistentVolumeClaim – that makes it independent on the actual type of the PersistentVolume. For
example, it can use HostPath volumes on Minikube or Amazon EBS volumes in Amazon AWS deployments without any
changes in the YAML files.

To deploy a Kafka cluster, create a ConfigMap with the cluster configuration and following labels:

```yaml
strimzi.io/type: kafka
strimzi.io/kind: cluster
```

For more details about the ConfigMap format, have a look 
into the example ConfigMaps for [ephemeral](resources/kubernetes/kafka-ephemeral.yaml) and 
[persistent](resources/kubernetes/kafka-persistent.yaml) clusters.

### Deploying to OpenShift

Kafka broker is provided in the form of an OpenShift template. The cluster can be deployed from the template either
using command line or using the OpenShift console. To create the ephemeral cluster, run following command in your
terminal:

```shell
oc new-app strimzi-ephemeral
```

To deploy the persistent Kafka cluster, run:

```shell
oc new-app strimzi-persistent
```

### Deploying to Kubernetes

To deploy Kafka broker on Kubernetes, the corresponding ConfigMap has to be created. To create the ephemeral
cluster, run following command in your terminal:

```shell
kubectl apply -f resources/kubernetes/kafka-ephemeral.yaml
```

To deploy the persistent Kafka cluster, run:

```shell
kubectl apply -f resources/kubernetes/kafka-persistent.yaml
```

## Kafka Connect

The Cluster Controller can also deploy a [Kafka Connect][connect] cluster which can be used with either of the Kafka
broker deployments described above. It is implemented as a deployment with a configurable number of workers.
The default image currently contains only the Connectors distributed with Apache Kafka Connect -`FileStreamSinkConnector`
and `FileStreamSourceConnector`. The REST interface for managing the Kafka Connect cluster is exposed internally within
the Kubernetes/OpenShift cluster as service `kafka-connect` on port `8083`.

### Deploying to OpenShift

Kafka Connect is provided in the form of OpenShift template. It can be deployed from the template either
using command line or using the OpenShift console. To create Kafka Connect cluster, run following command in your
terminal:

```shell
oc new-app strimzi-connect
```

### Deploying to Kubernetes

To deploy Kafka Connect on Kubernetes, the corresponding ConfigMap has to be created. Create the ConfigMap using
following command:

```shell
kubectl apply -f resources/kubernetes/kafka-connect.yaml
```

### Using Kafka Connect with additional plugins

Our Kafka Connect Docker images contain by default only the `FileStreamSinkConnector` and 
`FileStreamSourceConnector` connectors which are part of the Apache Kafka project.

Kafka Connect will automatically load all plugins/connectors which are present in the `/opt/kafka/plugins`
directory during startup. You can use our Kafka Connect S2I deployment to build a new Docker image containing additional plugins.:

- Use the `strimzi/kafka-connect` image as Docker base image, add your connectors to the `/opt/kafka/plugins/`
  directory and use this new image instead of `strimzi/kafka-connect` in the regular Kafka Connect deployment.
- Use OpenShift build system and our S2I image

#### Create a new image based on `strimzi/kafka-connect`

1. Create a new `Dockerfile` which uses `strimzi/kafka-connect`

```Dockerfile
FROM strimzi/kafka-connect:latest
USER root:root
COPY ./my-plugin/ /opt/kafka/plugins/
USER kafka:kafka
```

2. Build the Docker image and upload it to your Docker repository
3. Use your new Docker image in your Kafka Connect deployment
  * On OpenShift, use the parameters `IMAGE_REPO_NAME`, `IMAGE_NAME` and `IMAGE_TAG` to specify your custom Docker image
  * On Kubernetes, edit the [ConfigMap](resources/kubernetes/kafka-connect.yaml) and specify your Docker image.

#### Using OpenShift Build and S2I image

OpenShift supports [Builds](https://docs.openshift.org/3.6/dev_guide/builds/index.html) which can be used together
with [Source-to-Image (S2I)](https://docs.openshift.org/3.6/creating_images/s2i.html#creating-images-s2i) framework
to create new Docker images. OpenShift Build takes a builder image with the S2I support together with source code
and/or binaries provided by the user and uses them to build a new Docker image. 
The newly created Docker Image will be stored in OpenShift's local Docker repository and can be used in
deployments. The Strimzi project provides a Kafka Connect S2I builder image [`strimzi/kafka-connect-s2i`](https://hub.docker.com/r/strimzi/kafka-connect-s2i/) 
which takes user-provided binaries (with plugins and connectors) and creates a new Kafka Connect image.
This enhanced Kafka Connect image can be used with our Kafka Connect deployment.

To configure the OpenShift Build and create a new Kafka Connect image, follow these steps:

1. Create OpenShift build configuration and Kafka Connect deployment using our OpenShift template

```shell
oc new-app strimzi-connect-s2i
```

2. Prepare a directory with Kafka Connect plugins which you want to use. For example:

```shell
$ tree ./my-plugins/
./my-plugins/
├── debezium-connector-mongodb
│   ├── bson-3.4.2.jar
│   ├── CHANGELOG.md
│   ├── CONTRIBUTE.md
│   ├── COPYRIGHT.txt
│   ├── debezium-connector-mongodb-0.7.1.jar
│   ├── debezium-core-0.7.1.jar
│   ├── LICENSE.txt
│   ├── mongodb-driver-3.4.2.jar
│   ├── mongodb-driver-core-3.4.2.jar
│   └── README.md
├── debezium-connector-mysql
│   ├── CHANGELOG.md
│   ├── CONTRIBUTE.md
│   ├── COPYRIGHT.txt
│   ├── debezium-connector-mysql-0.7.1.jar
│   ├── debezium-core-0.7.1.jar
│   ├── LICENSE.txt
│   ├── mysql-binlog-connector-java-0.13.0.jar
│   ├── mysql-connector-java-5.1.40.jar
│   ├── README.md
│   └── wkb-1.0.2.jar
└── debezium-connector-postgres
    ├── CHANGELOG.md
    ├── CONTRIBUTE.md
    ├── COPYRIGHT.txt
    ├── debezium-connector-postgres-0.7.1.jar
    ├── debezium-core-0.7.1.jar
    ├── LICENSE.txt
    ├── postgresql-42.0.0.jar
    ├── protobuf-java-2.6.1.jar
    └── README.md
```

3. Start new image build using the prepared directory

```shell
oc start-build my-connect-cluster-connect --from-dir ./my-plugins/
```

_The name of the build should be changed according to your cluster name._

4. Once the build is finished, the new image will be automatically used with your Kafka Connect deployment.

## Metrics

Each Kafka broker and Zookeeper server pod exposes metrics by means of a [Prometheus][prometheus] endpoint. A JMX exporter,
running as a Java agent, is in charge of getting metrics from the pod (both JVM metrics and metrics strictly related to the
broker) and exposing them as such an endpoint.

The [Metrics](/metrics/METRICS.md) page details all the information for deploying a Prometheus server in the cluster in order
to scrape the pods and obtain metrics. The same page describes how to set up a [Grafana][grafana] instance to have a dashboard
showing the main configured metrics.

[kafka]: https://kafka.apache.org "Apache Kafka"
[connect]: https://kafka.apache.org/documentation/#connect "Apache Kafka Connect"
[k8s]: https://kubernetes.io/ "Kubernetes"
[os]: https://www.openshift.com/ "OpenShift"
[statefulset]: https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/ "Kubernetes StatefulSets"
[origin]: https://github.com/openshift/origin/releases "OpenShift Origin releases"
[minikube]: https://kubernetes.io/docs/getting-started-guides/minikube/ "Minikube"
[minishift]: https://docs.openshift.org/latest/minishift/index.html "Minishift"
[kubectl]: https://kubernetes.io/docs/tasks/tools/install-kubectl/ "Kubectl"
[occlusterup]: https://github.com/openshift/origin/blob/master/docs/cluster_up_down.md "Local Cluster Management"
[k8srwomany]: https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes
[prometheus]: https://prometheus.io/ "Prometheus"
[grafana]: https://grafana.com/ "Grafana"
