---
availability:
  - place: custom-resources
domain: kafka-connect
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaConnectAssemblyOperator.java#KafkaConnectAssemblyOperator
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/ConnectBuildOperator.java#ConnectBuildOperator
---

# Edit Kafka Connect

Change a Kafka Connect cluster's replicas, configuration, plugins, connection to Kafka, or whether KafkaConnector resources manage its connectors.
