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

# Add a listener

Give clients a new way to connect to a Kafka cluster by adding a listener to its `Kafka` resource.
