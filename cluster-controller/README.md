# Cluster Controller

The cluster controller is in charge to deploy a Kafka cluster alongside a Zookeeper ensemble. It's also able to deploy a 
Kafka Connect cluster which can connect to an already existing Kafka cluster.

When up and running, it starts to "watch" for ConfigMaps containing the Kafka or Kafka Connect cluster configuration.
Such a ConfigMaps have to have a specific label which by default is `strimzi.io/kind=cluster` (as described later) but 
it can be changed through a corresponding environment variable.

When a new ConfigMap is created on the Kubernetes/OpenShift cluster, the controller gets the deployment configuration from
its `data` section and starts the creation process of a new cluster (Kafka or Kafka Connect). It's done creating a bunch 
of Kubernetes/OpenShift resources needed for such a deployment, like StatefulSets, ConfigMaps, Services and so on.

Every time the ConfigMap is edited and updated, due to some changes in the `data` section, the controller executes a related updating
operation on the cluster (i.e. scale up/down, changing metrics configuration and so on). It means that some resources are 
patched or deleted and then re-created for reflecting the changes in the cluster configuration. 

Finally, if the ConfigMap is deleted, the controller starts to un-deploy the cluster deleting all the related Kubernets/OpenShift
resources.

## Reconciliation

TBD

## Format of the cluster ConfigMap

By default, the controller watches for ConfigMap(s) having the label `strimzi.io/kind=cluster` in order to find and get
configuration for a Kafka or Kafka Connect cluster to deploy. The label is configurable through the `STRIMZI_CONFIGMAP_LABELS` 
environment variable.

In order to distinguish which "type" of cluster to deploy, Kafka or Kafka Connect, the controller checks the
`strimzi.io/type` label which can have the following values :

* `kafka`: the ConfigMap provides configuration for a Kafka cluster (with Zookeeper ensemble) deployment
* `kafka-connect`: the ConfigMap provides configuration for a Kafka Connect cluster deployment
* `kafka-connect-s2i`: the ConfigMap provides configuration for a Kafka Connect cluster deployment using Build and Source2Image
features (works only with OpenShift)

The `data` section of such ConfigMaps contains different keys depending on the "type" of deployment as described in the 
following paragraphs.

### Kafka

TBD

### Kafka Connect

TBD

### Kafka Connect S2I

TBD

## Controller configuration

The controller itself can be configured through the following environment variables.

* `STRIMZI_CONFIGMAP_LABELS`: the Kubernetes/OpenShift label selector used to identify ConfigMaps to be managed by the controller.
Default: `strimzi.io/kind=cluster`.  
* `STRIMZI_FULL_RECONCILIATION_INTERVAL` : the interval between periodic reconciliations.
Default: 120000 ms
