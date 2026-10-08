---
domain: kafka-clusters
relations:
  - entity: kafka-node
    verb: has
    cardinality: one-to-many
references:
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/nodepool/KafkaNodePoolSpec.java#KafkaNodePoolSpec
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/nodepool/KafkaNodePoolStatus.java#KafkaNodePoolStatus
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/model/nodepools/NodePoolUtils.java#NodePoolUtils
  - kind: doc
    role: context
    target: documentation/modules/configuring/con-config-node-pools.adoc
---

# Node pool

A group of Kafka nodes with the same roles, storage and resources, declared by a `KafkaNodePool` resource that names its Kafka cluster.

## Information kept

- **Name** — the name of the KafkaNodePool resource
- **Roles** — controller, broker or both
- **Replicas** — how many nodes the pool runs
- **Storage** — ephemeral, persistent claim or JBOD volumes, with whether claims are deleted with the cluster and which volume holds the KRaft metadata log
- **Resources** — CPU and memory requests and limits, and JVM options
- **Template** — customizations of the pods and other resources generated for the pool
- **Node IDs** — the node IDs the pool's nodes use, reported in status
- **Next node IDs** — the node IDs or ranges to use when nodes are added
- **Node IDs to remove** — the node IDs or ranges to remove first when nodes are removed
- **Manual rolling update** — a request to roll every node of the pool
- **Status message** — the reason and message of the latest warning condition, such as a refused storage change
