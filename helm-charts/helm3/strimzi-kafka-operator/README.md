# Strimzi: Kafka as a Service

Strimzi provides a way to run an [Apache Kafka®](https://kafka.apache.org) cluster on 
[Kubernetes](https://kubernetes.io/) or [OpenShift](https://www.openshift.com/) in various deployment configurations.
See our [website](https://strimzi.io) for more details about the project.

## Introduction

This chart bootstraps the Strimzi Cluster Operator Deployment, Cluster Roles, Cluster Role Bindings, Service Accounts, and
Custom Resource Definitions for running [Apache Kafka](https://kafka.apache.org/) on [Kubernetes](http://kubernetes.io)
cluster using the [Helm](https://helm.sh) package manager.

### Supported Features

* **Manages the Kafka Cluster** - Deploys and manages all of the components of this complex application, including dependencies like Apache ZooKeeper® that are traditionally hard to administer.
* **Includes Kafka Connect** - Allows for configuration of common data sources and sinks to move data into and out of the Kafka cluster.
* **Topic Management** - Creates and manages Kafka Topics within the cluster.
* **User Management** - Creates and manages Kafka Users within the cluster.
* **Connector Management** - Creates and manages Kafka Connect connectors.
* **Includes Kafka Mirror Maker** - Allows for mirroring data between different Apache Kafka® clusters.
* **Includes HTTP Kafka Bridge** - Allows clients to send and receive messages through an Apache Kafka® cluster via the HTTP protocol.
* **Includes Cruise Control** - Automates the process of balancing partitions across an Apache Kafka® cluster.

### Upgrading your Clusters

The Strimzi Operator understands how to run and upgrade between a set of Kafka versions.
When specifying a new version in your config, check to make sure you aren't using any features that may have been removed.
See [the upgrade guide](https://strimzi.io/docs/latest/#assembly-upgrading-kafka-versions-str) for more information.

### Documentation

Documentation to all releases can be found on our [website](https://strimzi.io/documentation).

### Getting help

If you encounter any issues while using Strimzi, you can get help using:
* [Strimzi mailing list on CNCF](https://lists.cncf.io/g/cncf-strimzi-users/topics)
* [Strimzi Slack channel on CNCF workspace](https://cloud-native.slack.com/messages/strimzi)

### License

Strimzi is licensed under the [Apache License, Version 2.0](https://github.com/strimzi/strimzi-kafka-operator/blob/master/LICENSE).

## Prerequisites

- Kubernetes 1.11+

## Installing the Chart

Add the Strimzi Helm Chart repository:

```bash
$ helm repo add strimzi https://strimzi.io/charts/
```

To install the chart with the release name `my-release`:

```bash
$ helm install my-release strimzi/strimzi-kafka-operator
```

The command deploys the Strimzi Cluster Operator on the Kubernetes cluster with the default configuration.
The [configuration](#configuration) section lists the parameters that can be configured during installation.

## Uninstalling the Chart

To uninstall/delete the `my-release` deployment:

```bash
$ helm delete my-release
```

The command removes all the Kubernetes components associated with the operator and deletes the release.

## Configuration

The following table lists the configurable parameters of the Strimzi chart and their default values.  Runtime
configuration of Kafka and other components are defined within their respective Custom Resource Definitions.  See
the documentation for more details.

| Parameter                            | Description                               | Default                                              |
| ------------------------------------ | ----------------------------------------- | ---------------------------------------------------- |
| `watchNamespaces`                    | Comma separated list of additional namespaces for the strimzi-operator to watch | []             |
| `image.registry`                     | Cluster Operator image registry           | `quay.io`                                            |
| `image.repository`                   | Cluster Operator image repository         | `strimzi`                                            |
| `image.name`                         | Cluster Operator image name               | `cluster-operator`                                   |
| `image.tag`                          | Cluster Operator image tag                | `0.22.1`                                             |
| `image.imagePullPolicy`              | Image pull policy for all pods deployed by Cluster Operator       | `IfNotPresent`               |
| `image.imagePullSecrets`             | Docker registry pull secret               | `nil`                                                |
| `fullReconciliationIntervalMs`       | Full reconciliation interval in milliseconds | 120000                                            |
| `operationTimeoutMs`                 | Operation timeout in milliseconds         | 300000                                               |
| `operatorNamespaceLabels`            | Labels of the namespace where the operator runs | `nil`                                          |
| `zookeeper.image.registry  `         | ZooKeeper image registry                  | `quay.io`                                            |
| `zookeeper.image.repository`         | ZooKeeper image repository                | `strimzi`                                            |
| `zookeeper.image.name`               | ZooKeeper image name                      | `kafka`                                              |
| `zookeeper.image.tag`                | ZooKeeper image tag prefix                | `0.22.1`                                             |
| `jmxtrans.image.registry`            | JmxTrans image registry                   | `quay.io`                                            |
| `jmxtrans.image.repository`          | JmxTrans image repository                 | `strimzi`                                            |
| `jmxtrans.image.name`                | JmxTrans image name                       | `jmxtrans`                                           |
| `jmxtrans.image.tag`                 | JmxTrans image tag prefix                 | `0.22.1`                                             |
| `kafka.image.registry`               | Kafka image registry                      | `quay.io`                                            |
| `kafka.image.repository`             | Kafka image repository                    | `strimzi`                                            |
| `kafka.image.name`                   | Kafka image name                          | `kafka`                                              |
| `kafka.image.tagPrefix`              | Kafka image tag prefix                    | `0.22.1`                                             |
| `kafkaConnect.image.registry`        | Kafka Connect image registry              | `quay.io`                                            |
| `kafkaConnect.image.repository`      | Kafka Connect image repository            | `strimzi`                                            |
| `kafkaConnect.image.name`            | Kafka Connect image name                  | `kafka`                                              |
| `kafkaConnect.image.tagPrefix`       | Kafka Connect image tag prefix            | `0.22.1`                                             |
| `kafkaConnects2i.image.registry`     | Kafka Connect s2i image registry          | `quay.io`                                            |
| `kafkaConnects2i.image.repository`   | Kafka Connect s2i image repository        | `strimzi`                                            |
| `kafkaConnects2i.image.name`         | Kafka Connect s2i image name              | `kafka`                                              |
| `kafkaConnects2i.image.tagPrefix`    | Kafka Connect s2i image tag prefix        | `0.22.1`                                             |
| `kafkaMirrorMaker.image.registry`    | Kafka Mirror Maker image registry         | `quay.io`                                            |
| `kafkaMirrorMaker.image.repository`  | Kafka Mirror Maker image repository       | `strimzi`                                            |
| `kafkaMirrorMaker.image.name`        | Kafka Mirror Maker image name             | `kafka`                                              |
| `kafkaMirrorMaker.image.tagPrefix`   | Kafka Mirror Maker image tag prefix       | `0.22.1`                                             |
| `cruiseControl.image.registry`       | Cruise Control image registry             | `quay.io`                                            |
| `cruiseControl.image.repository`     | Cruise Control image repository           | `strimzi`                                            |
| `cruiseControl.image.name`           | Cruise Control image name                 | `kafka`                                              |
| `cruiseControl.image.tag`            | Cruise Control image tag prefix           | `0.22.1`                                             |
| `topicOperator.image.registry`       | Topic Operator image registry             | `quay.io`                                            |
| `topicOperator.image.repository`     | Topic Operator image repository           | `strimzi`                                            |
| `topicOperator.image.name`           | Topic Operator image name                 | `operator`                                           |
| `topicOperator.image.tag`            | Topic Operator image tag                  | `0.22.1`                                             |
| `userOperator.image.registry`        | User Operator image registry              | `quay.io`                                            |
| `userOperator.image.repository`      | User Operator image repository            | `strimzi`                                            |
| `userOperator.image.name`            | User Operator image name                  | `operator`                                           |
| `userOperator.image.tag`             | User Operator image tag                   | `0.22.1`                                             |
| `kafkaInit.image.registry`           | Init Kafka image registry                 | `quay.io`                                            |
| `kafkaInit.image.repository`         | Init Kafka image repository               | `strimzi`                                            |
| `kafkaInit.image.name`               | Init Kafka image name                     | `operator`                                           |
| `kafkaInit.image.tag`                | Init Kafka image tag                      | `0.22.1`                                             |
| `tlsSidecarCruiseControl.image.registry` | TLS Sidecar for Cruise Control image registry | `quay.io`                                    |
| `tlsSidecarCruiseControl.image.repository` | TLS Sidecar for Cruise Control image repository | `strimzi`                                |
| `tlsSidecarCruiseControl.image.name`     | TLS Sidecar for Cruise Control image name      | `kafka`                                     |
| `tlsSidecarCruiseControl.image.tag`      | TLS Sidecar for Cruise Control image tag prefix | `0.22.1`                                   |
| `tlsSidecarTopicOperator.image.registry` | TLS Sidecar for Topic Operator image registry | `quay.io`                                    |
| `tlsSidecarTopicOperator.image.repository` | TLS Sidecar for Topic Operator image repository | `strimzi`                                |
| `tlsSidecarTopicOperator.image.name` | TLS Sidecar for Topic Operator image name | `kafka`                                              |
| `tlsSidecarTopicOperator.image.tag`  | TLS Sidecar for Topic Operator image tag prefix | `0.22.1`                                       |
| `kafkaBridge.image.registry`         | Kafka Bridge image registry               | `quay.io`                                            |
| `kafkaBridge.image.repository`       | Kafka Bridge image repository             | `strimzi`                                            |
| `kafkaBridge.image.name`             | Kafka Bridge image name                   | `kafka-bridge                                        |
| `kafkaBridge.image.tag`              | Kafka Bridge image tag                    | `0.19.0`                                             |
| `kanikoExecutor.image.registry`      | Kaniko Executor image registry            | `quay.io`                                            |
| `kanikoExecutor.image.repository`    | Kaniko Executor image repository          | `strimzi`                                            |
| `kanikoExecutor.image.name`          | Kaniko Executor image name                | `kaniko-executor`                                    |
| `kanikoExecutor.image.tag`           | Kaniko Executor image tag                 | `0.22.1`                                             |
| `resources.limits.memory`            | Memory constraint for limits              | `256Mi`                                              |
| `resources.limits.cpu`               | CPU constraint for limits                 | `1000m`                                              |
| `resources.requests.memory`          | Memory constraint for requests            | `256Mi`                                              |
| `livenessProbe.initialDelaySeconds`  | Liveness probe initial delay in seconds   | 10                                                   |
| `livenessProbe.periodSeconds`        | Liveness probe period in seconds          | 30                                                   |
| `readinessProbe.initialDelaySeconds` | Readiness probe initial delay in seconds  | 10                                                   |
| `readinessProbe.periodSeconds`       | Readiness probe period in seconds         | 30                                                   |
| `imageRegistryOverride`              | Override all image registry config        | `nil`                                                |
| `imageRepositoryOverride`            | Override all image repository config      | `nil`                                                |
| `imageTagOverride`                   | Override all image tag config             | `nil`                                                |
| `createGlobalResources`              | Allow creation of cluster-scoped resources| `true`                                               |
| `tolerations`                        | Add tolerations to Operator Pod           | `[]`                                                 |
| `affinity`                           | Add affinities to Operator Pod            | `{}`                                                 |
| `annotations`                        | Add annotations to Operator Pod           | `{}`                                                 |
| `labels`                             | Add labels to Operator Pod                | `{}`                                                 |
| `nodeSelector`                       | Add a node selector to Operator Pod       | `{}`                                                 |

Specify each parameter using the `--set key=value[,key=value]` argument to `helm install`. For example,

```bash
$ helm install --name my-release --set logLevel=DEBUG,fullReconciliationIntervalMs=240000 strimzi/strimzi-kafka-operator
```
