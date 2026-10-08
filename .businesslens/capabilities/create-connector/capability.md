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
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/AbstractConnectOperator.java#AbstractConnectOperator
  - kind: doc
    role: context
    target: documentation/modules/deploying/proc-deploying-kafkaconnector.adoc
---

# Create a connector

Declare a connector with a `KafkaConnector` resource labelled with the Kafka Connect cluster it runs in. Strimzi creates it through the Kafka Connect REST API in the requested state.
