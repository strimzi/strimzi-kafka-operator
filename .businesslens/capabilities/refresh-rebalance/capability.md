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
    target: documentation/modules/cruise-control/proc-fixing-problems-with-kafkarebalance.adoc
---

# Refresh a rebalance

Ask Cruise Control for a fresh proposal, for example because the cluster changed since the last one or the previous attempt failed or was stopped.
