---
availability:
  - place: custom-resources
domain: kafka-clusters
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/model/nodepools/NodePoolUtils.java#NodePoolUtils
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/model/nodepools/NodeIdAssignor.java#NodeIdAssignor
---

# Create a node pool

Add a node pool to an existing Kafka cluster, for example to run brokers on different hardware or storage.
