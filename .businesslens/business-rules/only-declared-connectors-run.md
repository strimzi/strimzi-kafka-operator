---
appliesTo:
  - type: entity
    id: connect-cluster
  - type: entity
    id: connector
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaConnectAssemblyOperator.java#KafkaConnectAssemblyOperator
---

# A Kafka Connect cluster with connector resources enabled runs only the connectors KafkaConnector resources declare

While a Kafka Connect cluster enables connector resources, every reconciliation deletes connectors that exist in it without a KafkaConnector resource, including ones created through the Kafka Connect REST API.
