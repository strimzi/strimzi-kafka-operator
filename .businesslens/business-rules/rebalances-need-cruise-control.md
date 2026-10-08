---
appliesTo:
  - type: entity
    id: rebalance
    effect: changes
    to: Pending proposal
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaRebalanceAssemblyOperator.java#reconcileKafkaRebalance
---

# Rebalance proposals are requested only for Ready Kafka clusters with Cruise Control deployed

A rebalance moves to Pending proposal — when it is requested, refreshed or resumed — only when its labelled Kafka cluster exists, is Ready and deploys Cruise Control; otherwise it is reported Not ready.
