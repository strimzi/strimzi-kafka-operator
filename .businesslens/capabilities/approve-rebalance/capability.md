---
availability:
  - place: custom-resources
domain: rebalancing
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaRebalanceAssemblyOperator.java#KafkaRebalanceAssemblyOperator
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/rebalance/KafkaRebalanceState.java#KafkaRebalanceState
---

# Approve a rebalance

Approve a ready proposal so that Cruise Control carries out the rebalance.
