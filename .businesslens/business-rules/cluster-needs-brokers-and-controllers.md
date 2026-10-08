---
appliesTo:
  - type: entity
    id: kafka-cluster
  - type: entity
    id: node-pool
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/model/nodepools/NodePoolUtils.java#validateNodePools
---

# A Kafka cluster runs only with node pools that provide both brokers and controllers

A Kafka cluster needs at least one node pool with the broker role and one with the controller role, each with at least one replica, in its namespace and labelled with its name; otherwise its Kafka resource is Not ready and no Kafka node changes.
