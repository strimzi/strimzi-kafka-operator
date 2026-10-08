---
availability:
  - place: custom-resources
domain: kafka-clusters
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaClusterCreator.java#KafkaClusterCreator
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/model/nodepools/NodeIdAssignor.java#NodeIdAssignor
  - kind: doc
    role: context
    target: documentation/modules/configuring/proc-scaling-down-node-pools.adoc
---

# Remove Kafka nodes

Remove Kafka nodes from a node pool by lowering its replicas. Strimzi removes the highest node IDs first, or the IDs the pool names, and only removes brokers once they host no partition replicas.
