---
availability:
  - place: custom-resources
domain: kafka-connect
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaConnectAssemblyOperator.java#KafkaConnectAssemblyOperator
  - kind: doc
    role: context
    target: documentation/modules/deploying/proc-deploy-kafka-connect-using-kafka-connect-build.adoc
---

# Rebuild the Kafka Connect image

Rebuild the image of a Kafka Connect cluster that uses a build — for example to pick up a newer base image or plugin artifacts — without changing the `KafkaConnect` resource.
