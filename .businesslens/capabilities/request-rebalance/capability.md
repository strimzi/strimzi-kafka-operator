---
availability:
  - place: custom-resources
domain: rebalancing
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaRebalanceAssemblyOperator.java#KafkaRebalanceAssemblyOperator
  - kind: doc
    role: context
    target: documentation/modules/cruise-control/proc-generating-optimization-proposals.adoc
---

# Request a rebalance

Ask Cruise Control for a rebalance proposal by creating a `KafkaRebalance` resource labelled with a Kafka cluster: a full rebalance, moving replicas onto added brokers, off brokers about to be removed, or off broker volumes.
