---
availability:
  - place: custom-resources
domain: kafka-connect
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaConnectAssemblyOperator.java#KafkaConnectAssemblyOperator
---

# Delete Kafka Connect

Delete a Kafka Connect cluster by deleting its `KafkaConnect` resource.
