---
availability:
  - place: custom-resources
domain: kafka-clusters
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaReconciler.java#KafkaReconciler
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/resource/KafkaRoller.java#KafkaRoller
---

# Edit a Kafka cluster

Change a Kafka cluster's authorization, broker configuration, rack awareness, tiered storage, quotas, metrics, logging, templates, maintenance time windows or the components deployed beside it. Strimzi applies what Kafka can change at runtime without restarts and rolls nodes one at a time for everything else, keeping partitions available.
