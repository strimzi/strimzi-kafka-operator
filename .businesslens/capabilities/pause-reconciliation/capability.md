---
availability:
  - place: custom-resources
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/AbstractOperator.java#reconcileResource
  - kind: doc
    role: context
    target: documentation/modules/operators/proc-pausing-reconciliation.adoc
---

# Pause reconciliation

Pause reconciliation of a `Kafka`, `KafkaConnect`, `KafkaConnector`, `KafkaMirrorMaker2`, `KafkaBridge`, `KafkaTopic`, `KafkaUser` or `KafkaRebalance` resource, so its operator ignores changes to it until the pause is removed. A resource can also be created already paused.
