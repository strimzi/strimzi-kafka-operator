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
  - kind: doc
    role: context
    target: documentation/modules/configuring/proc-managing-storage-node-pools.adoc
---

# Delete a node pool

Remove a node pool and its Kafka nodes from a Kafka cluster by deleting its `KafkaNodePool` resource.
