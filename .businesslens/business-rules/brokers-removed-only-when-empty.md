---
appliesTo:
  - type: capability
    id: remove-kafka-nodes
  - type: entity
    id: kafka-node
    effect: removes
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaClusterCreator.java#KafkaClusterCreator
  - kind: doc
    role: context
    target: documentation/modules/configuring/con-skipping-scale-down-checks.adoc
---

# Scaling down a node pool removes brokers only once they host no partition replicas

Lowering a node pool's replicas removes brokers only once they host no partition replicas. Otherwise the scale-down is reverted with a ScaleDownPreventionCheck warning, unless the Kafka resource carries the skip broker scale-down check annotation. Deleting a whole node pool is not checked.

## Rationale

Removing a broker that still hosts replicas would make those partitions under-replicated or unavailable.
