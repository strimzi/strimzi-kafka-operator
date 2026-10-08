---
appliesTo:
  - type: entity
    id: kafka-node
    effect: removes
  - type: entity
    id: node-pool
    effect: removes
  - type: entity
    id: kafka-cluster
    effect: removes
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/model/PersistentVolumeClaimUtils.java#PersistentVolumeClaimUtils
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/PvcReconciler.java#PvcReconciler
  - kind: doc
    role: context
    target: documentation/assemblies/deploying/assembly-cluster-recovery-volume.adoc
---

# Volume claims are deleted only where storage asks for their deletion

Persistent volume claims of removed Kafka nodes, and of a deleted node pool, are deleted only where the node pool's storage sets deleteClaim; otherwise they remain so that the data can be recovered. Deleting a Kafka resource alone deletes no volume claims.
