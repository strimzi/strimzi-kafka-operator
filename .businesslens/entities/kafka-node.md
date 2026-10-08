---
domain: kafka-clusters
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaClusterCreator.java#KafkaClusterCreator
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/resource/KafkaRoller.java#KafkaRoller
---

# Kafka node

One controller or broker of a Kafka cluster, identified by its node ID and run as one pod with its own volumes.

## Information kept

- **Node ID** — the Kafka node ID, unique in the cluster
- **Roles** — controller, broker or both, inherited from its node pool
- **Volumes** — the persistent volume claims holding the node's data
- **Partition replicas** — how many partition replicas the node hosts as a broker
- **Manual rolling update** — a request to restart this node
- **Pod and volume deletion** — a request to delete the node's pod and volume claims so it starts again with empty volumes
