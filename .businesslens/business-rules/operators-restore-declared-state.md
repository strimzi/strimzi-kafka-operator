---
appliesTo:
  - type: entity
    id: kafka-cluster
  - type: entity
    id: kafka-node
  - type: entity
    id: connect-cluster
  - type: entity
    id: mirror-maker-2
  - type: entity
    id: http-bridge
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/ClusterOperator.java#ClusterOperator
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/StrimziPodSetController.java#StrimziPodSetController
---

# Strimzi puts back what a resource declares when someone changes it by hand

The Cluster Operator reconciles every Kafka, KafkaConnect, KafkaMirrorMaker2, KafkaBridge and KafkaRebalance resource periodically, two minutes apart by default, and restores the Kubernetes resources it created for them when they were changed or deleted by hand; a deleted pod is recreated at once.
