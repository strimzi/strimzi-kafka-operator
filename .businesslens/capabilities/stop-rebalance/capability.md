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
  - kind: doc
    role: context
    target: documentation/modules/cruise-control/proc-stopping-cluster-rebalance.adoc
---

# Stop a rebalance

Stop a rebalance while its proposal is being prepared or while it is being carried out.
