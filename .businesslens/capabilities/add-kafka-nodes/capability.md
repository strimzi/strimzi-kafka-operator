---
availability:
  - place: custom-resources
domain: kafka-clusters
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/model/nodepools/NodeIdAssignor.java#NodeIdAssignor
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaAutoRebalancingReconciler.java#KafkaAutoRebalancingReconciler
---

# Add Kafka nodes

Add Kafka nodes to a node pool by raising its replicas. New brokers receive no partitions until a rebalance moves some to them; a Kafka cluster with auto-rebalancing on scaling runs that rebalance itself.
