# Strimzi: Apache Kafka on Kubernetes

Strimzi provides a way to run an [Apache Kafka®](https://kafka.apache.org) cluster on
[Kubernetes](https://kubernetes.io/) or [OpenShift](https://www.openshift.com/) in various deployment configurations.
See our [website](https://strimzi.io) for more details about the project.

## CRD Upgrades

**!!! IMPORTANT !!!**
Strimzi 0.23 and newer supports only the API version `v1beta2` of all Strimzi custom resources.
This is a required as part of the migration to `apiextensionsk8s.io/v1` which is needed because Kubernetes 1.22 will remove support for `apiextensions.k8s.io/v1beta1`.
Migration to `v1beta2` needs to be completed for all Strimzi CRDs and CRs before the upgrade to 0.23 or newer.
For more details about the CRD upgrades, see the [documentation](https://strimzi.io/docs/operators/0.24.0/deploying.html#assembly-upgrade-resources-str).

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
* **Includes Kafka Mirror Maker 1 and 2** - Allows for mirroring data between different Apache Kafka® clusters.
* **Includes HTTP Kafka Bridge** - Allows clients to send and receive messages through an Apache Kafka® cluster via the HTTP protocol.
* **Includes Cruise Control** - Automates the process of balancing partitions across an Apache Kafka® cluster.
* **Prometheus monitoring** - Built-in support for monitoring using Prometheus.

### Upgrading your Clusters

To upgrade the Strimzi operator, you can use the `helm upgrade` command.
The `helm upgrade` command does not upgrade the [Custom Resource Definitions](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/).
Install the new CRDs manually after upgrading the Cluster Operator.
You can access the CRDs from our [GitHub release page](https://github.com/strimzi/strimzi-kafka-operator/releases) or find them in the `crd` subdirectory inside the Helm Chart.

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

Strimzi is licensed under the [Apache License, Version 2.0](https://github.com/strimzi/strimzi-kafka-operator/blob/main/LICENSE).

## Prerequisites

- Kubernetes 1.16+

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
| `watchAnyNamespace`                  | Watch the whole Kubernetes cluster (all namespaces) | `false`                                    |
| `defaultImageRegistry`               | Default image registry for all the images | `quay.io`                                            |
| `defaultImageRepository`             | Default image registry for all the images | `strimzi`                                            |
| `defaultImageTag`                    | Default image tag for all the images except Kafka Bridge | `0.29.0`                              |
| `image.registry`                     | Override default Cluster Operator image registry  | `nil`                                        |
| `image.repository`                   | Override default Cluster Operator image repository  | `nil`                                      |
| `image.name`                         | Cluster Operator image name               | `cluster-operator`                                   |
| `image.tag`                          | Override default Cluster Operator image tag       | `nil`                                        |
| `image.imagePullPolicy`              | Image pull policy for all pods deployed by Cluster Operator       | `IfNotPresent`               |
| `image.imagePullSecrets`             | Docker registry pull secret               | `nil`                                                |
| `fullReconciliationIntervalMs`       | Full reconciliation interval in milliseconds | 120000                                            |
| `operationTimeoutMs`                 | Operation timeout in milliseconds         | 300000                                               |
| `operatorNamespaceLabels`            | Labels of the namespace where the operator runs | `nil`                                          |
| `podSecurityContext`                 | Cluster Operator pod's security context    | `nil`                                               |
| `priorityClassName`                  | Cluster Operator pod's priority class name | `nil`                                               |
| `securityContext`                    | Cluster Operator container's security context |  `nil`                                           |
| `extraEnvs`                          | Extra environment variables for the Cluster operator container | `[]`                            |
| `zookeeper.image.registry  `         | Override default ZooKeeper image registry | `nil`                                                |
| `zookeeper.image.repository`         | Override default ZooKeeper image repository | `nil`                                              |
| `zookeeper.image.name`               | ZooKeeper image name                      | `kafka`                                              |
| `zookeeper.image.tag`                | Override default ZooKeeper tag registry   | `nil`                                                |
| `jmxtrans.image.registry`            | Override default JmxTrans image registry                   | `nil`                               |
| `jmxtrans.image.repository`          | Override default JmxTrans image repository                 | `nil`                               |
| `jmxtrans.image.name`                | JmxTrans image name                       | `jmxtrans`                                           |
| `jmxtrans.image.tag`                 | Override default JmxTrans image tag prefix                 | `nil`                               |
| `kafka.image.registry`               | Override default Kafka image registry                      | `nil`                               |
| `kafka.image.repository`             | Override default Kafka image repository                    | `nil`                               |
| `kafka.image.name`                   | Kafka image name                          | `kafka`                                              |
| `kafka.image.tagPrefix`              | Override default Kafka image tag prefix                    | `nil`                               |
| `kafkaConnect.image.registry`        | Override default Kafka Connect image registry              | `nil`                               |
| `kafkaConnect.image.repository`      | Override default Kafka Connect image repository            | `nil`                               |
| `kafkaConnect.image.name`            | Kafka Connect image name                  | `kafka`                                              |
| `kafkaConnect.image.tagPrefix`       | Override default Kafka Connect image tag prefix            | `nil`                               |
| `kafkaMirrorMaker.image.registry`    | Override default Kafka Mirror Maker image registry         | `nil`                               |
| `kafkaMirrorMaker.image.repository`  | Override default Kafka Mirror Maker image repository       | `nil`                               |
| `kafkaMirrorMaker.image.name`        | Kafka Mirror Maker image name             | `kafka`                                              |
| `kafkaMirrorMaker.image.tagPrefix`   | Override default Kafka Mirror Maker image tag prefix       | `nil`                               |
| `cruiseControl.image.registry`       | Override default Cruise Control image registry             | `nil`                               |
| `cruiseControl.image.repository`     | Override default Cruise Control image repository           | `nil`                               |
| `cruiseControl.image.name`           | Cruise Control image name                 | `kafka`                                              |
| `cruiseControl.image.tag`            | Override default Cruise Control image tag prefix           | `nil`                               |
| `topicOperator.image.registry`       | Override default Topic Operator image registry             | `nil`                               |
| `topicOperator.image.repository`     | Override default  Topic Operator image repository          | `nil`                               |
| `topicOperator.image.name`           | Topic Operator image name                 | `operator`                                           |
| `topicOperator.image.tag`            | Override default Topic Operator image tag                  | `nil`                               |
| `userOperator.image.registry`        | Override default User Operator image registry              | `nil`                               |
| `userOperator.image.repository`      | Override default User Operator image repository            | `nil`                               |
| `userOperator.image.name`            | User Operator image name                  | `operator`                                           |
| `userOperator.image.tag`             | Override default User Operator image tag                   | `nil`                               |
| `kafkaInit.image.registry`           | Override default Init Kafka image registry                 | `nil`                               |
| `kafkaInit.image.repository`         | Override default Init Kafka image repository               | `nil`                               |
| `kafkaInit.image.name`               | Init Kafka image name                     | `operator`                                           |
| `kafkaInit.image.tag`                | Override default Init Kafka image tag                      | `nil`                               |
| `tlsSidecarTopicOperator.image.registry` | Override default TLS Sidecar for Topic Operator image registry | `nil`                       |
| `tlsSidecarTopicOperator.image.repository` | Override default TLS Sidecar for Topic Operator image repository | `nil`                   |
| `tlsSidecarTopicOperator.image.name` | TLS Sidecar for Topic Operator image name | `kafka`                                              |
| `tlsSidecarTopicOperator.image.tag`  | Override default TLS Sidecar for Topic Operator image tag prefix | `nil`                         |
| `kafkaBridge.image.registry`         | Override default Kafka Bridge image registry               | `quay.io`                           |
| `kafkaBridge.image.repository`       | Override default Kafka Bridge image repository             | `strimzi`                           |
| `kafkaBridge.image.name`             | Kafka Bridge image name                   | `kafka-bridge`                                       |
| `kafkaBridge.image.tag`              | Override default Kafka Bridge image tag                    | `0.21.5`                            |
| `kanikoExecutor.image.registry`      | Override default Kaniko Executor image registry            | `nil`                               |
| `kanikoExecutor.image.repository`    | Override default Kaniko Executor image repository          | `nil`                               |
| `kanikoExecutor.image.name`          | Kaniko Executor image name                | `kaniko-executor`                                    |
| `kanikoExecutor.image.tag`           | Override default Kaniko Executor image tag                 | `nil`                               |
| `resources.limits.memory`            | Memory constraint for limits              | `256Mi`                                              |
| `resources.limits.cpu`               | CPU constraint for limits                 | `1000m`                                              |
| `resources.requests.memory`          | Memory constraint for requests            | `256Mi`                                              |
| `livenessProbe.initialDelaySeconds`  | Liveness probe initial delay in seconds   | 10                                                   |
| `livenessProbe.periodSeconds`        | Liveness probe period in seconds          | 30                                                   |
| `readinessProbe.initialDelaySeconds` | Readiness probe initial delay in seconds  | 10                                                   |
| `readinessProbe.periodSeconds`       | Readiness probe period in seconds         | 30                                                   |
| `imageTagOverride`                   | Override all image tag config             | `nil`                                                |
| `createGlobalResources`              | Allow creation of cluster-scoped resources| `true`                                               |
| `tolerations`                        | Add tolerations to Operator Pod           | `[]`                                                 |
| `affinity`                           | Add affinities to Operator Pod            | `{}`                                                 |
| `annotations`                        | Add annotations to Operator Pod           | `{}`                                                 |
| `labels`                             | Add labels to Operator Pod                | `{}`                                                 |
| `nodeSelector`                       | Add a node selector to Operator Pod       | `{}`                                                 |
| `featureGates`                       | Feature Gates configuration               | `nil`                                                |
| `tmpDirSizeLimit`                    | Set the `sizeLimit` for the tmp dir volume used by the operator  | `1Mi`                         |
| `labelsExclusionPattern`             | Override the exclude pattern for exclude some labels             | `""`                          |
| `generateNetworkPolicy`              | Controls whether Strimzi generates network policy resources      | `true`                        |
| `connectBuildTimeoutMs`              | Overrides the default timeout value for building new Kafka Connect    | `300000`                 |
| `mavenBuilder.image.registry`        | Override default Maven Builder image registry              | `nil`                               |
| `mavenBuilder.image.repository`      | Maven Builder image repository            | `nil`                                                |
| `mavenBuilder.image.name`            | Override default Maven Builder image name                  | `maven-builder`                     |
| `mavenBuilder.image.tag`             | Override default Maven Builder image tag                   | `nil`                               |
| `logConfiguration`                   | Override default `log4j.properties` content                | `nil`                               |

Specify each parameter using the `--set key=value[,key=value]` argument to `helm install`. For example,

```bash
$ helm install --name my-release --set logLevel=DEBUG,fullReconciliationIntervalMs=240000 strimzi/strimzi-kafka-operator
```
