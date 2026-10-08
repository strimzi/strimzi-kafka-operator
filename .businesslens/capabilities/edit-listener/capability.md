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

# Edit a listener

Change the port, type, encryption, authentication, addresses or network policy of one of a Kafka cluster's listeners.
