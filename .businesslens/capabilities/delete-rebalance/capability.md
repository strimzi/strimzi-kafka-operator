---
availability:
  - place: custom-resources
domain: rebalancing
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaRebalanceAssemblyOperator.java#KafkaRebalanceAssemblyOperator
---

# Delete a rebalance

Delete a `KafkaRebalance` resource that is no longer needed.
