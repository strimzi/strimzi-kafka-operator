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

In order to configure a Kafka cluster deployment, it's possible to specify the following fields in the `data` section of 
the related ConfigMap :

* `kafka-nodes`: number of Kafka broker nodes
* `kafka-image`: the Docker image to use for the Kafka brokers. Default is `strimzi/kafka:latest`
* `kafka-healthcheck-delay`: the initial delay for the liveness and readiness probes for each Kafka broker node
* `kafka-healthcheck-timeout`: the timeout on the liveness and readiness probes for each Kafka broker node
* `zookeeper-nodes`: number of Zookeeper nodes
* `zookeeper-image`: the Docker image to use for the Zookeeper nodes. Default is `strimzi/zookeeper:latest`
* `zookeeper-healthcheck-delay`: the initial delay for the liveness and readiness probes for each Zookeeper node
* `zookeeper-healthcheck-timeout`: the timeout on the liveness and readiness probes for each Zookeeper node
* `KAFKA_DEFAULT_REPLICATION_FACTOR`: the default replication factors for automatically created topics. It sets the 
`default.replication.factor` property in the properties configuration file used by Kafka broker nodes on startup
* `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR`: the replication factor for the offsets topic. It sets the  
`offsets.topic.replication.factor` property in the properties configuration file used by Kafka broker nodes on startup
* `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR`: the replication factor for the transaction topic. It sets the 
`transaction.state.log.replication.factor` property in the properties configuration file used by Kafka broker nodes on startup
* `kafka-storage`: a JSON string representing the storage configuration for the Kafka broker nodes. See related section
* `zookeeper-storage`: a JSON string representing the storage configuration for the Zookeeper nodes. See related section
* `kafka-metrics-config`: a JSON string representing the JMX exporter configuration for exposing metrics from Kafka broker nodes.
 Removing this field means having no metrics exposed.
* `zookeeper-metrics-config`: a JSON string representing the JMX exporter configuration for exposing metrics from Zookeeper nodes.
 Removing this field means having no metrics exposed.
 
An example of cluster configuration ConfigMap is the following.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-cluster
  labels:
    strimzi.io/kind: cluster
    strimzi.io/type: kafka
data:
  kafka-nodes: "3"
  kafka-image: "strimzi/kafka:latest"
  kafka-healthcheck-delay: "15"
  kafka-healthcheck-timeout: "5"
  zookeeper-nodes: "1"
  zookeeper-image: "strimzi/zookeeper:latest"
  zookeeper-healthcheck-delay: "15"
  zookeeper-healthcheck-timeout: "5"
  KAFKA_DEFAULT_REPLICATION_FACTOR: "3"
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "3"
  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "3"
  kafka-storage: |-
    { "type": "ephemeral" }
  zookeeper-storage: |-
    { "type": "ephemeral" }
  kafka-metrics-config: |-
    {
      "lowercaseOutputName": true,
      "rules": [
          {
            "pattern": "kafka.server<type=(.+), name=(.+)PerSec\\w*><>Count",
            "name": "kafka_server_$1_$2_total"
          },
          {
            "pattern": "kafka.server<type=(.+), name=(.+)PerSec\\w*, topic=(.+)><>Count",
            "name": "kafka_server_$1_$2_total",
            "labels":
            {
              "topic": "$3"
            }
          }
      ]
    }
  zookeeper-metrics-config: |-
    {
      "lowercaseOutputName": true
    }
```

#### Storage

Kafka needs to save information like data logs (messages), consumer offsets and so on in some way; the same is true for 
Zookeeper about znodes values.
Strimzi allows to save such a data in an "ephemeral" way so using `emptyDir` or in a "persistent-claim" way using persistent 
volume claims.
It's possible to provide the storage configuration in the related ConfigMap using a JSON string as value for fields 
`kafka-storage` and `zookeeper-storage`.

The JSON representation has a mandatory `type` field for specifying the type of storage to use ("ephemeral" or "persistent-claim").
In case of "persistent-claim" type the following fields can be used as well :

* `size`: defines the size of the persistent volume claim (i.e 1Gi)
* `class` : the Kubernetes/OpenShift storage class to use for dynamic volume allocation
* `selector`: allows to select a specific persistent volume to use. It contains a `matchLabels` field which defines an 
inner JSON object with key:value representing labels for selecting such a volume.
* `delete-claim`: specifies if the persistent volume claim has to be deleted when the cluster is un-deployed. Default is `false`

The "ephemeral" storage is really simple to configure and the related JSON string is something like this.

```json
{ "type": "ephemeral" }

``` 

The "persistent" storage has some more parameters but other than `type`, the `size` is the only mandatory one.

```json
{ "type": "persistent-claim", "size": "1Gi" }
```

A more complex configuration could use a storage class in the following way.

```json
{
  "type": "persistent-claim",
  "size": "1Gi",
  "class": "my-storage-class"
}
```

Finally, a selector can be used in order to select the needs for a specific labeled persistent volume.

```json
{
  "type": "persistent-claim",
  "size": "1Gi",
  "selector":
  {
    "matchLabels":
    {
      "hdd-type": "ssd"
    }
  },
  "delete-claim": "true"
}
```

#### Metrics

Because the Strimzi project uses the [JMX exporter](https://github.com/prometheus/jmx_exporter) in order to expose metrics 
on each node, the JSON string used for metrics configuration in the cluster ConfigMap reflects the related JMX exporter 
configuration file. For this reason, you can find more information on how to use it in the corresponding GitHub repo.

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
