# Strimzi: Kafka as a Service

Strimzi provides a way to run an [Apache Kafka](https://kafka.apache.org/) cluster on
[Kubernetes](https://kubernetes.io/) or [OpenShift](https://www.openshift.com/) in various deployment configurations.
See our [website](https://github.com/strimzi/strimzi-kafka-operator) for more details about the project.

## Introduction

This chart bootstraps the Strimzi Cluster Operator Deployment, Cluster Roles, Cluster Role Bindings, Service Accounts, and
Custom Resource Definitions for running [Apache Kafka](https://kafka.apache.org/) on [Kubernetes](http://kubernetes.io)
cluster using the [Helm](https://helm.sh) package manager.

## Prerequisites

- Kubernetes 1.9+
- PV provisioner support in the underlying infrastructure

## Installing the Chart

Add the Strimzi Helm Chart repository:

```bash
$ helm repo add strimzi http://strimzi.io/charts/
```

To install the chart with the release name `my-release`:

```bash
$ helm install --name my-release strimzi/strimzi-kafka-operator
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
| `image.repository`                   | Cluster Operator image repository         | `strimzi`                                            |
| `image.name`                         | Cluster Operator image name               | `cluster-operator`                                   |
| `image.tag`                          | Cluster Operator image tag                | `latest`                                             |
| `image.imagePullPolicy`              | Cluster Operator image pull policy        | `IfNotPresent`                                       |
| `logLevel`                           | Cluster Operator log level                | `INFO`                                               |
| `fullReconciliationIntervalMs`       | Full reconciliation interval in milliseconds | 120000                                            |
| `operationTimeoutMs`                 | Operation timeout in milliseconds         | 300000                                               |
| `zookeeper.image.repository`         | ZooKeeper image repository                | `strimzi`                                            |
| `zookeeper.image.name`               | ZooKeeper image name                      | `kafka`                                              |
| `zookeeper.image.tag`                | ZooKeeper image tag prefix                | `latest`                                             |
| `kafka.image.repository`             | Kafka image repository                    | `strimzi`                                            |
| `kafka.image.name`                   | Kafka image name                          | `kafka`                                              |
| `kafka.image.tagPrefix`              | Kafka image tag prefix                    | `latest`                                             |
| `kafkaConnect.image.repository`      | Kafka Connect image repository            | `strimzi`                                            |
| `kafkaConnect.image.name`            | Kafka Connect image name                  | `kafka`                                              |
| `kafkaConnect.image.tagPrefix`       | Kafka Connect image tag prefix            | `latest`                                             |
| `kafkaConnects2i.image.repository`   | Kafka Connect s2i image repository        | `strimzi`                                            |
| `kafkaConnects2i.image.name`         | Kafka Connect s2i image name              | `kafka`                                              |
| `kafkaConnects2i.image.tagPrefix`    | Kafka Connect s2i image tag prefix        | `latest`                                             |
| `kafkaMirrorMaker.image.repository`  | Kafka Mirror Maker image repository       | `strimzi`                                            |
| `kafkaMirrorMaker.image.name`        | Kafka Mirror Maker image name             | `kafka`                                              |
| `kafkaMirrorMaker.image.tagPrefix`   | Kafka Mirror Maker image tag prefix       | `latest`                                             |
| `topicOperator.image.repository`     | Topic Operator image repository           | `strimzi`                                            |
| `topicOperator.image.name`           | Topic Operator image name                 | `operator`                                           |
| `topicOperator.image.tag`            | Topic Operator image tag                  | `latest`                                             |
| `userOperator.image.repository`      | User Operator image repository            | `strimzi`                                            |
| `userOperator.image.name`            | User Operator image name                  | `operator`                                           |
| `userOperator.image.tag`             | User Operator image tag                   | `latest`                                             |
| `kafkaInit.image.repository`         | Init Kafka image repository               | `strimzi`                                            |
| `kafkaInit.image.name`               | Init Kafka image name                     | `operator`                                           |
| `kafkaInit.image.tag`                | Init Kafka image tag                      | `latest`                                             |
| `tlsSidecarZookeeper.image.repository` | TLS Sidecar for ZooKeeper image repository | `strimzi`                                         |
| `tlsSidecarZookeeper.image.name`     | TLS Sidecar for ZooKeeper image name      | `kafka`                                              |
| `tlsSidecarZookeeper.image.tag`      | TLS Sidecar for ZooKeeper image tag prefix | `latest`                                            |
| `tlsSidecarKafka.image.repository`   | TLS Sidecar for Kafka image repository    | `strimzi`                                            |
| `tlsSidecarKafka.image.name`         | TLS Sidecar for Kafka image name          | `kafka`                                              |
| `tlsSidecarKafka.image.tag`          | TLS Sidecar for Kafka image tag prefix    | `latest`                                             |
| `tlsSidecarTopicOperator.image.repository` | TLS Sidecar for Topic Operator image repository | `strimzi`                                |
| `tlsSidecarTopicOperator.image.name` | TLS Sidecar for Topic Operator image name | `kafka`                                              |
| `tlsSidecarTopicOperator.image.tag`  | TLS Sidecar for Topic Operator image tag prefix | `latest`                                       |
| `kafkaBridge.image.repository`       | Kafka Bridge image repository             | `strimzi`                                            |
| `kafkaBridge.image.name`             | Kafka Bridge image name                   | `kafka-bridge                                        |
| `kafkaBridge.image.tag`              | Kafka Bridge image tag                    | `latest`                                             |
| `resources.limits.memory`            | Memory constraint for limits              | `256Mi`                                              |
| `resources.limits.cpu`               | CPU constraint for limits                 | `1000m`                                              |
| `resources.requests.memory`          | Memory constraint for requests            | `256Mi`                                              |
| `livenessProbe.initialDelaySeconds`  | Liveness probe initial delay in seconds   | 10                                                   |
| `livenessProbe.periodSeconds`        | Liveness probe period in seconds          | 30                                                   |
| `readinessProbe.initialDelaySeconds` | Readiness probe initial delay in seconds  | 10                                                   |
| `readinessProbe.periodSeconds`       | Readiness probe period in seconds         | 30                                                   |
| `imageRepositoryOverride`            | Override all image repository config      | `nil`                                                |
| `imageTagOverride`                   | Override all image tag config             | `nil`                                                |
| `createGlobalResources`              | Allow creation of cluster-scoped resources| `true`                                               |

Specify each parameter using the `--set key=value[,key=value]` argument to `helm install`. For example,

```bash
$ helm install --name my-release --set logLevel=DEBUG,fullReconciliationIntervalMs=240000 strimzi/strimzi-kafka-operator
```
