# Kafka as a Service

Strimzi provides a way to run an [Apache Kafka][kafka] cluster on 
[Kubernetes][k8s] or [OpenShift][os] in various deployment configurations.

<!-- TOC depthFrom:2 -->

- [Deploying a Kubernetes/OpenShift cluster](#deploying-a-kubernetesopenshift-cluster)
    - [Kubernetes](#kubernetes)
    - [OpenShift](#openshift)
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
        - [Create a new image based on `strimzi/kafka-connect`](#create-a-new-image-based-on-strimzikafka-connect)
        - [Using Openshift Build and S2I image](#using-openshift-build-and-s2i-image)
- [Metrics](#metrics)

<!-- /TOC -->

## Deploying a Kubernetes/OpenShift cluster

You will need a Kubernetes or OpenShift cluster to deploy Strimzi.

### Kubernetes

In order to interact with a Kubernetes cluster, be sure to have [the kubectl tool][kubectl] installed.
If you don't have a Kubernetes cluster up and running, the easiest way to deploy one, for development purposes, is to use the [Minikube][minikube] project which provides a single-node cluster in a VM. In order to do that you can just follow the installation guide which describes all the prerequisites and how to get the Minikube binaries.
Finally, the cluster can be deployed by running

```
minikube start
```

### OpenShift

In order to interact with an OpenShift cluster, be sure to have the [OpenShift client tools][origin] installed.
If you don't have an OpenShift cluster up and running, the easiest way to deploy one, for development purposes, is to use the [Minishift][minishift] project which provides a single-node cluster in a VM. In order to do that you can just follow the installation guide which describes all the prerequisites and how to get the Minishift binaries.
Finally, the cluster can be deployed by running

```
minishift start
```

Another way is to use the OpenShift client tools directly to spin-up a single-node cluster. It will run OpenShift as a Docker container on the local machine.

```
oc cluster up
```

More information about this way can be found [here][occlusterup].

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

1. Create the [provided "strimzi" template](kafka-statefulsets/resources/openshift-template.yaml) 
   by running

        oc create -f kafka-statefulsets/resources/openshift-template.yaml

   in your terminal. This template provides the "zookeeper" StatefulSet  with 3 replicas, the "kafka" StatefulSet with 3 replicas,
   and the "zookeeper", "zookeeper-headless", "kafka" and "kafka-headless" Services.
2. Create a new app using the "strimzi" template:

        oc new-app strimzi

### Deploying to Kubernetes

1. If your cluster doesn't have any default storage class, create the persistent volumes manually

        kubectl apply -f kafka-statefulsets/resources/cluster-volumes.yaml

2. Create the services by running:

        kubectl apply -f kafka-statefulsets/resources/kubernetes.yaml

3. You can then verify that the services started using

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

1. Create a pod using the [provided template](kafka-inmemory/resources/openshift-template.yaml) by running

        oc create -f kafka-inmemory/resources/openshift-template.yaml

   in your terminal. This template provides the "zookeeper" and the "kafka" deployments and the 
   "zookeeper-service" and "kafka-service" services. 
2. Create a new app:

        oc new-app strimzi


### Deploying to Kubernetes

1. Create the deployments and services by running:

        kubectl apply -f kafka-inmemory/resources/kubernetes.yaml

2. You can then verify that the services started using

        kubectl describe all


## Kafka Connect

This deployment adds a [Kafka Connect][connect] cluster which can be used with either of the Kafka deployments described above. 
It is implemented as a deployment with a configurable number of workers. 
The default image currently contains only the Connectors distributed with Apache Kafka Connect - 
`FileStreamSinkConnector` and `FileStreamSourceConnector`. 
The REST interface for managing the Kafka Connect cluster is exposed internally within the Kubernetes/OpenShift 
cluster as service `kafka-connect` on port `8083`.


### Deploying to OpenShift

1. Deploy a Kafka broker to your OpenShift cluster using either of the [in-memory](#kafka-in-memory) or 
   [statefulsets deployments](#kafka-stateful-sets) above.
2. Create a pod using the [provided template](kafka-connect/resources/openshift-template.yaml) by running

        oc create -f kafka-connect/resources/openshift-template.yaml

   in your terminal.
3. Create a new app:

        oc new-app strimzi-connect


### Deploying to Kubernetes

1. Deploy a Kafka broker to your Kubernetes cluster using either of the [in-memory](#kafka-in-memory) or 
   [statefulsets deployments](#kafka-stateful-sets) above.
2. Start the deployment by running 

        kubectl apply -f kafka-connect/resources/kubernetes.yaml

   in your terminal.


### Using Kafka Connect with additional plugins

Our Kafka Connect Docker images contain by default only the `FileStreamSinkConnector` and 
`FileStreamSourceConnector` connectors which are part of the Apache Kafka project.

Kafka Connect will automatically load all plugins/connectors which are present in the `/opt/kafka/plugins` 
directory during startup. You can use several different methods how to add the plugins into this directory:

* Mount a volume containing the plugins to path `/opt/kafka/plugins/`
* Use the `strimzi/kafka-connect` image as Docker base image, add your connectors to the `/opt/kafka/plugins/` 
  directory and use this new image instead of `strimzi/kafka-connect`
* Use OpenShift build system and our S2I image

#### Mount a volume containing the plugins

You can distribute your plugins to all your cluster nodes into the same path, make them 
world-readable (`chmod -R a+r /path/to/your/directory`) and use the hostPath volume to mount them into 
your Kafka Connect deployment. To use the volume, you have to edit the deployment YAML files:

1. Open the [OpenShift template](kafka-connect/resources/openshift-template.yaml) 
or [Kubernetes deployment](kafka-connect/resources/kubernetes.yaml)
2. add the `volumeMounts` and `volumes` sections in the same way as in the example below
3. Redeploy Kafka Connect for the changes to take effect (If you have Kafka Connect already deployed, you have to apply 
the changes to the deployment and afterwards make sure all pods are restarted. If you haven't yet deployed Kakfa Connect, 
just follow the guide above and use your modified YAML files.)

```yaml
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: kafka-connect
spec:
  replicas: 1
  template:
    metadata:
      labels:
        name: kafka-connect
    spec:
      containers:
        - name: kafka-connect
          image: strimzi/kafka-connect:latest
          ports:
            - name: rest-api
              containerPort: 8083
              protocol: TCP
          env:
            - name: KAFKA_CONNECT_BOOTSTRAP_SERVERS
              value: "kafka:9092"
          livenessProbe:
            httpGet:
              path: /
              port: rest-api
            initialDelaySeconds: 60
          volumeMounts:
            - mountPath: /opt/kafka/plugins
              name: pluginsvol
      volumes:
        - name: pluginsvol
          hostPath:
            path: /path/to/my/plugins
            type: Directory
```

Alternatively, you can create Kubernetes/OpenShift persistent volume which contains additional plugins and modify the 
Kafka Connect deployment to use this volume. Since distributed Kafka Connect cluster can run on multiple nodes you need 
to make sure that the volume can be mounted as read only into multiple pods at the same time. Which volume types 
can be mounted read only on several pods can be found in [Kubernetes documentation][k8srwomany]. Once you have 
such volume, you can edit the deployment YAML file as described above and just use your persistent volume instead of 
the hostPath volume. For example for GlusterFS, you can use:

```yaml
volumes:
  - name: pluginsvol
    glusterfs: 
      endpoints: glusterfs-cluster
      path: kube_vol
      readOnly: true
```

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

#### Using Openshift Build and S2I image

OpenShift supports [Builds](https://docs.openshift.org/3.6/dev_guide/builds/index.html) which can be used together 
with [Source-to-Image (S2I)](https://docs.openshift.org/3.6/creating_images/s2i.html#creating-images-s2i) framework to 
create new Docker images. OpenShift Build takes a builder image with the S2I support together with source code 
and/or binaries provided by the user and uses them to build a new Docker image. 
The newly created Docker Image will be stored in OpenShift's local Docker repository and can be used in deployments. 
The Strimzi project provides a Kafka Connect S2I builder image [`strimzi/kafka-connect-s2i`](https://hub.docker.com/r/strimzi/kafka-connect-s2i/) 
which takes user-provided binaries (with plugins and connectors) and creates a new Kafka Connect image. 
This enhanced Kafka Connect image can be used with our Kafka Connect deployment.

To configure the OpenShift Build and create a new Kafka Connect image, follow these steps:

1. Create OpenShift build configuration and Kafka Connect deployment using our OpenShift template
```
oc apply -f kafka-connect/s2i/resources/openshift-template.yaml
oc new-app strimzi-connect-s2i
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

## Metrics

Each Kafka broker and Zookeeper server pod exposes metrics by means of a [Prometheus][prometheus] endpoint. A JMX exporter, running as a Java agent, is in charge of getting metrics from the pod (both JVM metrics and metrics strictly related to the broker) and exposing them as such an endpoint.

The [Metrics](/metrics/METRICS.md) page details all the information for deploying a Prometheus server in the cluster in order to scrape the pods and obtain metrics. The same page describes how to set up a [Grafana][grafana] instance to have a dashboard showing the main configured metrics.

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
