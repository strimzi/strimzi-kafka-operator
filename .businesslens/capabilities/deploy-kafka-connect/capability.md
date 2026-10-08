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
  - kind: doc
    role: context
    target: documentation/assemblies/deploying/assembly-deploy-kafka-connect.adoc
---

# Deploy Kafka Connect

Create a Kafka Connect cluster from a `KafkaConnect` resource, optionally with an image Strimzi builds to include connector plugins.
