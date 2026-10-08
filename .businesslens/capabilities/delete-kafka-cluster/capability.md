---
availability:
  - place: custom-resources
domain: kafka-clusters
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/model/PersistentVolumeClaimUtils.java#PersistentVolumeClaimUtils
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/CaProvider.java#CaProvider
---

# Delete a Kafka cluster

Delete a Kafka cluster by deleting its `Kafka` resource. Kubernetes removes the pods, services and components Strimzi created for it.
