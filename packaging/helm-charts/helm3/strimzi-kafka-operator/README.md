# Strimzi: Apache Kafka on Kubernetes

Strimzi provides a way to run an [Apache Kafka®](https://kafka.apache.org) cluster on
[Kubernetes](https://kubernetes.io/) or [OpenShift](https://www.openshift.com/) in various deployment configurations.
See our [website](https://strimzi.io) for more details about the project.

**!!! IMPORTANT !!!**

* **Strimzi 0.45 is the last Strimzi version with support for ZooKeeper-based Apache Kafka clusters and MirrorMaker 1 deployments.**
  **Please make sure to [migrate to KRaft](https://strimzi.io/docs/operators/latest/full/deploying.html#assembly-kraft-mode-str) and MirrorMaker 2 before upgrading to Strimzi 0.46 or newer.**
* Strimzi 0.45 is the last Strimzi version to include the [Strimzi EnvVar Configuration Provider](https://github.com/strimzi/kafka-env-var-config-provider) (deprecated in Strimzi 0.38.0) and [Strimzi MirrorMaker 2 Extensions](https://github.com/strimzi/mirror-maker-2-extensions) (deprecated in Strimzi 0.28.0).
  Please use the Apache Kafka [EnvVarConfigProvider](https://github.com/strimzi/kafka-env-var-config-provider?tab=readme-ov-file#deprecation-notice) and [Identity Replication Policy](https://github.com/strimzi/mirror-maker-2-extensions?tab=readme-ov-file#identity-replication-policy) instead.
* From Strimzi 0.44.0 on, we support only Kubernetes 1.25 and newer.
  Kubernetes 1.23 and 1.24 are not supported anymore.
* Upgrading to Strimzi 0.32 and newer directly from Strimzi 0.22 and earlier is no longer possible.
  Please follow the [documentation](https://strimzi.io/docs/operators/latest/full/deploying.html#assembly-upgrade-str) for more details.

## Introduction

This chart bootstraps the Strimzi Cluster Operator Deployment, Cluster Roles, Cluster Role Bindings, Service Accounts, and
Custom Resource Definitions for running [Apache Kafka](https://kafka.apache.org/) on [Kubernetes](http://kubernetes.io)
cluster using the [Helm](https://helm.sh) package manager.

### Supported Features

* **Manages the Kafka Cluster** - Deploys and manages all of the components of this complex application, including dependencies like Apache ZooKeeper® that are traditionally hard to administer.
* **KRaft support** - Allows running Apache Kafka clusters in the KRaft mode (without ZooKeeper).
* **Includes Kafka Connect** - Allows for configuration of common data sources and sinks to move data into and out of the Kafka cluster.
* **Topic Management** - Creates and manages Kafka Topics within the cluster.
* **User Management** - Creates and manages Kafka Users within the cluster.
* **Connector Management** - Creates and manages Kafka Connect connectors.
* **Includes Kafka MirrorMaker** - Allows for mirroring data between different Apache Kafka® clusters.
* **Includes HTTP Kafka Bridge** - Allows clients to send and receive messages through an Apache Kafka® cluster via the HTTP protocol.
* **Includes Cruise Control** - Automates the process of balancing partitions across an Apache Kafka® cluster.
* **Auto-rebalancing when scaling** - Automatically rebalance the Kafka cluster after a scale-up or before a scale-down.
* **Tiered storage** - Offloads older, less critical data to a lower-cost, lower-performance storage tier, such as object storage.
* **Prometheus monitoring** - Built-in support for monitoring using Prometheus.
* **Grafana Dashboards** - Built-in support for loading Grafana® dashboards via the grafana_sidecar

### Upgrading your Clusters

To upgrade the Strimzi operator, you can use the `helm upgrade` command.
The `helm upgrade` command does not upgrade the [Custom Resource Definitions](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/).
Install the new CRDs manually after upgrading the Cluster Operator.
You can access the CRDs from our [GitHub release page](https://github.com/strimzi/strimzi-kafka-operator/releases) or find them in the `crd` subdirectory inside the Helm Chart.

The Strimzi Operator understands how to run and upgrade between a set of Kafka versions.
When specifying a new version in your config, check to make sure you aren't using any features that may have been removed.
See [the upgrade guide](https://strimzi.io/docs/operators/latest/deploying.html#assembly-upgrading-kafka-versions-str) for more information.

### Documentation

Documentation to all releases can be found on our [website](https://strimzi.io/documentation).

### Getting help

If you encounter any issues while using Strimzi, you can get help using:
* [Strimzi mailing list on CNCF](https://lists.cncf.io/g/cncf-strimzi-users/topics)
* [Strimzi Slack channel on CNCF workspace](https://cloud-native.slack.com/messages/strimzi)
* [GitHub Discussions](https://github.com/strimzi/strimzi-kafka-operator/discussions)

### License

Strimzi is licensed under the [Apache License, Version 2.0](https://github.com/strimzi/strimzi-kafka-operator/blob/main/LICENSE).

## Prerequisites

- Kubernetes 1.25+

## Installing the Chart

To install the chart with the release name `my-strimzi-cluster-operator`:

```bash
$ helm install my-strimzi-cluster-operator oci://quay.io/strimzi-helm/strimzi-kafka-operator
```

The command deploys the Strimzi Cluster Operator on the Kubernetes cluster with the default configuration.
The [configuration](#configuration) section lists the parameters that can be configured during installation.

## Uninstalling the Chart

To uninstall/delete the `my-strimzi-cluster-operator` deployment:

```bash
$ helm delete my-strimzi-cluster-operator
```

The command removes all the Kubernetes components associated with the operator and deletes the release.

## Configuration

The following table lists the configurable parameters of the Strimzi chart and their default values.  Runtime
configuration of Kafka and other components are defined within their respective Custom Resource Definitions.  See
the documentation for more details.

| Parameter                                   | Description                                                                     | Default                      |
|---------------------------------------------|---------------------------------------------------------------------------------|------------------------------|
| `replicas`                                  | Number of replicas of the cluster operator                                      | 1                            |
| `revisionHistoryLimit`                      | Number of replicaSet to keep of the operator deployment                         | 10                           |
| `watchNamespaces`                           | Comma separated list of additional namespaces for the strimzi-operator to watch | []                           |
| `watchAnyNamespace`                         | Watch the whole Kubernetes cluster (all namespaces)                             | `false`                      |
| `defaultImageRegistry`                      | Default image registry for all the images                                       | `quay.io`                    |
| `defaultImageRepository`                    | Default image registry for all the images                                       | `strimzi`                    |
| `defaultImageTag`                           | Default image tag for all the images except Kafka Bridge                        | `0.45.0`                     |
| `image.registry`                            | Override default Cluster Operator image registry                                | `nil`                        |
| `image.repository`                          | Override default Cluster Operator image repository                              | `nil`                        |
| `image.name`                                | Cluster Operator image name                                                     | `cluster-operator`           |
| `image.tag`                                 | Override default Cluster Operator image tag                                     | `nil`                        |
| `image.digest`                              | Override Cluster Operator image tag with digest                                 | `nil`                        |
| `image.imagePullPolicy`                     | Image pull policy for all pods deployed by Cluster Operator                     | `IfNotPresent`               |
| `image.imagePullSecrets`                    | List of Docker registry pull secrets                                            | `[]`                         |
| `fullReconciliationIntervalMs`              | Full reconciliation interval in milliseconds                                    | 120000                       |
| `leaderElection.enable`                     | Whether to enable leader election                                               | `true`                       |
| `operationTimeoutMs`                        | Operation timeout in milliseconds                                               | 300000                       |
| `operatorNamespaceLabels`                   | Labels of the namespace where the operator runs                                 | `nil`                        |
| `podSecurityContext`                        | Cluster Operator pod's security context                                         | `nil`                        |
| `priorityClassName`                         | Cluster Operator pod's priority class name                                      | `nil`                        |
| `securityContext`                           | Cluster Operator container's security context                                   | `nil`                        |
| `rbac.create`                               | Whether to create RBAC related resources                                        | `yes`                        |
| `serviceAccountCreate`                      | Whether to create a service account                                             | `yes`                        |
| `serviceAccount`                            | Cluster Operator's service account                                              | `strimzi-cluster-operator`   |
| `podDisruptionBudget.enabled`               | Whether to enable the podDisruptionBudget feature                               | `false`                      |
| `podDisruptionBudget.minAvailable`          | Default value for how many pods must be running in a cluster                    | `1`                          |
| `podDisruptionBudget.maxUnavailable`        | Default value for how many pods can be down                                     | `nil`                        |
| `extraEnvs`                                 | Extra environment variables for the Cluster operator container                  | `[]`                         |
| `kafka.image.registry`                      | Override default Kafka image registry                                           | `nil`                        |
| `kafka.image.repository`                    | Override default Kafka image repository                                         | `nil`                        |
| `kafka.image.name`                          | Kafka image name                                                                | `kafka`                      |
| `kafka.image.tagPrefix`                     | Override default Kafka image tag prefix                                         | `nil`                        |
| `kafka.image.tag`                           | Override default Kafka image tag and ignore suffix                              | `nil`                        |
| `kafka.image.digest`                        | Override Kafka image tag with digest                                            | `nil`                        |
| `kafkaConnect.image.registry`               | Override default Kafka Connect image registry                                   | `nil`                        |
| `kafkaConnect.image.repository`             | Override default Kafka Connect image repository                                 | `nil`                        |
| `kafkaConnect.image.name`                   | Kafka Connect image name                                                        | `kafka`                      |
| `kafkaConnect.image.tagPrefix`              | Override default Kafka Connect image tag prefix                                 | `nil`                        |
| `kafkaConnect.image.tag`                    | Override default Kafka Connect image tag and ignore suffix                      | `nil`                        |
| `kafkaConnect.image.digest`                 | Override Kafka Connect image tag with digest                                    | `nil`                        |
| `kafkaMirrorMaker.image.registry`           | Override default Kafka Mirror Maker image registry                              | `nil`                        |
| `kafkaMirrorMaker.image.repository`         | Override default Kafka Mirror Maker image repository                            | `nil`                        |
| `kafkaMirrorMaker.image.name`               | Kafka Mirror Maker image name                                                   | `kafka`                      |
| `kafkaMirrorMaker.image.tagPrefix`          | Override default Kafka Mirror Maker image tag prefix                            | `nil`                        |
| `kafkaMirrorMaker.image.tag`                | Override default Kafka Mirror Maker image tag and ignore suffix                 | `nil`                        |
| `kafkaMirrorMaker.image.digest`             | Override Kafka Mirror Maker image tag with digest                               | `nil`                        |
| `cruiseControl.image.registry`              | Override default Cruise Control image registry                                  | `nil`                        |
| `cruiseControl.image.repository`            | Override default Cruise Control image repository                                | `nil`                        |
| `cruiseControl.image.name`                  | Cruise Control image name                                                       | `kafka`                      |
| `cruiseControl.image.tagPrefix`             | Override default Cruise Control image tag prefix                                | `nil`                        |
| `cruiseControl.image.tag`                   | Override default Cruise Control image tag and ignore suffix                     | `nil`                        |
| `cruiseControl.image.digest`                | Override Cruise Control image tag with digest                                   | `nil`                        |
| `topicOperator.image.registry`              | Override default Topic Operator image registry                                  | `nil`                        |
| `topicOperator.image.repository`            | Override default  Topic Operator image repository                               | `nil`                        |
| `topicOperator.image.name`                  | Topic Operator image name                                                       | `operator`                   |
| `topicOperator.image.tag`                   | Override default Topic Operator image tag                                       | `nil`                        |
| `topicOperator.image.digest`                | Override Topic Operator image tag with digest                                   | `nil`                        |
| `userOperator.image.registry`               | Override default User Operator image registry                                   | `nil`                        |
| `userOperator.image.repository`             | Override default User Operator image repository                                 | `nil`                        |
| `userOperator.image.name`                   | User Operator image name                                                        | `operator`                   |
| `userOperator.image.tag`                    | Override default User Operator image tag                                        | `nil`                        |
| `userOperator.image.digest`                 | Override User Operator image tag with digest                                    | `nil`                        |
| `kafkaInit.image.registry`                  | Override default Init Kafka image registry                                      | `nil`                        |
| `kafkaInit.image.repository`                | Override default Init Kafka image repository                                    | `nil`                        |
| `kafkaInit.image.name`                      | Init Kafka image name                                                           | `operator`                   |
| `kafkaInit.image.tag`                       | Override default Init Kafka image tag                                           | `nil`                        |
| `kafkaInit.image.digest`                    | Override Init Kafka image tag with digest                                       | `nil`                        |
| `kafkaBridge.image.registry`                | Override default Kafka Bridge image registry                                    | `quay.io`                    |
| `kafkaBridge.image.repository`              | Override default Kafka Bridge image repository                                  | `strimzi`                    |
| `kafkaBridge.image.name`                    | Kafka Bridge image name                                                         | `kafka-bridge`               |
| `kafkaBridge.image.tag`                     | Override default Kafka Bridge image tag                                         | `0.31.1`                     |
| `kafkaBridge.image.digest`                  | Override Kafka Bridge image tag with digest                                     | `nil`                        |
| `kafkaExporter.image.registry`              | Override default Kafka Exporter image registry                                  | `nil`                        |
| `kafkaExporter.image.repository`            | Override default Kafka Exporter image repository                                | `nil`                        |
| `kafkaExporter.image.name`                  | Kafka Exporter image name                                                       | `kafka`                      |
| `kafkaExporter.image.tagPrefix`             | Override default Kafka Exporter image tag prefix                                | `nil`                        |
| `kafkaExporter.image.tag`                   | Override default Kafka Exporter image tag and ignore suffix                     | `nil`                        |
| `kafkaExporter.image.digest`                | Override Kafka Exporter image tag with digest                                   | `nil`                        |
| `kafkaMirrorMaker2.image.registry`          | Override default Kafka Mirror Maker 2 image registry                            | `nil`                        |
| `kafkaMirrorMaker2.image.repository`        | Override default Kafka Mirror Maker 2 image repository                          | `nil`                        |
| `kafkaMirrorMaker2.image.name`              | Kafka Mirror Maker 2 image name                                                 | `kafka`                      |
| `kafkaMirrorMaker2.image.tagPrefix`         | Override default Kafka Mirror Maker 2 image tag prefix                          | `nil`                        |
| `kafkaMirrorMaker2.image.tag`               | Override default Kafka Mirror Maker 2 image tag and ignore suffix               | `nil`                        |
| `kafkaMirrorMaker2.image.digest`            | Override Kafka Mirror Maker 2 image tag with digest                             | `nil`                        |
| `kanikoExecutor.image.registry`             | Override default Kaniko Executor image registry                                 | `nil`                        |
| `kanikoExecutor.image.repository`           | Override default Kaniko Executor image repository                               | `nil`                        |
| `kanikoExecutor.image.name`                 | Kaniko Executor image name                                                      | `kaniko-executor`            |
| `kanikoExecutor.image.tag`                  | Override default Kaniko Executor image tag                                      | `nil`                        |
| `kanikoExecutor.image.digest`               | Override Kaniko Executor image tag with digest                                  | `nil`                        |
| `resources.limits.memory`                   | Memory constraint for limits                                                    | `384Mi`                      |
| `resources.limits.cpu`                      | CPU constraint for limits                                                       | `1000m`                      |
| `resources.requests.memory`                 | Memory constraint for requests                                                  | `384Mi`                      |
| `livenessProbe.initialDelaySeconds`         | Liveness probe initial delay in seconds                                         | 10                           |
| `livenessProbe.periodSeconds`               | Liveness probe period in seconds                                                | 30                           |
| `readinessProbe.initialDelaySeconds`        | Readiness probe initial delay in seconds                                        | 10                           |
| `readinessProbe.periodSeconds`              | Readiness probe period in seconds                                               | 30                           |
| `imageTagOverride`                          | Override all image tag config                                                   | `nil`                        |
| `createGlobalResources`                     | Allow creation of cluster-scoped resources                                      | `true`                       |
| `createAggregateRoles`                      | Create cluster roles that extend aggregated roles to use Strimzi CRDs           | `false`                      |
| `tolerations`                               | Add tolerations to Operator Pod                                                 | `[]`                         |
| `affinity`                                  | Add affinities to Operator Pod                                                  | `{}`                         |
| `annotations`                               | Add annotations to Operator Pod                                                 | `{}`                         |
| `labels`                                    | Add labels to Operator Pod                                                      | `{}`                         |
| `nodeSelector`                              | Add a node selector to Operator Pod                                             | `{}`                         |
| `featureGates`                              | Feature Gates configuration                                                     | `nil`                        |
| `tmpDirSizeLimit`                           | Set the `sizeLimit` for the tmp dir volume used by the operator                 | `1Mi`                        |
| `labelsExclusionPattern`                    | Override the exclude pattern for exclude some labels                            | `""`                         |
| `generateNetworkPolicy`                     | Controls whether Strimzi generates network policy resources                     | `true`                       |
| `connectBuildTimeoutMs`                     | Overrides the default timeout value for building new Kafka Connect              | `300000`                     |
| `mavenBuilder.image.registry`               | Override default Maven Builder image registry                                   | `nil`                        |
| `mavenBuilder.image.repository`             | Maven Builder image repository                                                  | `nil`                        |
| `mavenBuilder.image.name`                   | Override default Maven Builder image name                                       | `maven-builder`              |
| `mavenBuilder.image.tag`                    | Override default Maven Builder image tag                                        | `nil`                        |
| `mavenBuilder.image.digest`                 | Override Maven Builder image tag with digest                                    | `nil`                        |
| `logConfiguration`                          | Override default `log4j.properties` content                                     | `nil`                        |
| `logLevel`                                  | Override default logging level                                                  | `INFO`                       |
| `dashboards.enable`                         | Generate configmaps containing the dashboards                                   | `false`                      |
| `dashboards.label`                          | How should the dashboards be labeled for the sidecar                            | `grafana_dashboard`          |
| `dashboards.labelValue`                     | What should the dashboards label value be for the sidecar                       | `"1"`                        |
| `dashboards.extraLabels`                    | Any additional labels you would like on the dashboards                          | `{}`                         |
| `dashboards.namespace`                      | What namespace should the dashboards be loaded into                             | `Follows toplevel Namespace` |
| `dashboards.annotations`                    | Any custom annotations (such as folder for the sidecar)                         | `{}`                         |

Specify each parameter using the `--set key=value[,key=value]` argument to `helm install`. For example,

```bash
$ helm install my-strimzi-cluster-operator --set replicas=2 oci://quay.io/strimzi-helm/strimzi-kafka-operator
```
