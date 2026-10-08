---
availability:
  - place: custom-resources
domain: kafka-clusters
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/model/ListenersValidator.java#ListenersValidator
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaListenersReconciler.java#KafkaListenersReconciler
---

# Remove a listener

Stop clients connecting through one of a Kafka cluster's listeners by removing it from the `Kafka` resource.
