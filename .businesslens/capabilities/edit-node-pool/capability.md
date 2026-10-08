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
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/model/StorageDiff.java#StorageDiff
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/PvcReconciler.java#PvcReconciler
---

# Edit a node pool

Change a node pool's roles, resources, storage or templates.
