---
availability:
  - place: custom-resources
domain: kafka-clusters
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/ManualPodCleaner.java#ManualPodCleaner
  - kind: doc
    role: context
    target: documentation/modules/configuring/proc-manual-delete-pod-pvc-kafka.adoc
---

# Delete a node's pod and volume

Ask Strimzi to delete a Kafka node's pod together with its persistent volume claims, so the node starts again on new, empty volumes — for example to move it to other storage or recover from a broken disk.
