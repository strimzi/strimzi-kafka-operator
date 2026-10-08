---
appliesTo:
  - type: entity
    id: node-pool
    effect: changes
    facts:
      - Storage
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/model/StorageDiff.java#StorageDiff
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/model/KafkaPool.java#KafkaPool
---

# Node pool storage only grows

A node pool's storage may only change by growing persistent volumes, adding or removing JBOD volumes, changing whether claims are deleted, and moving the KRaft metadata log to another single volume. Any other storage change — a different type or storage class, or a smaller volume — is ignored as a whole and reported as a warning on the node pool and the Kafka cluster.
