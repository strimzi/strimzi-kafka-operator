---
appliesTo:
  - type: capability
    id: create-connector
  - type: entity
    id: connector
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaConnectAssemblyOperator.java#reconcileConnector
---

# KafkaConnector resources run only in Kafka Connect clusters with connector resources enabled and workers

Strimzi creates and manages a connector from a KafkaConnector resource only when its labelled Kafka Connect cluster exists, enables connector resources and has at least one worker; otherwise the resource is reported Not ready with the reason.
