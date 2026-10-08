---
availability:
  - place: custom-resources
domain: kafka-clusters
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaAssemblyOperator.java#KafkaAssemblyOperator
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaReconciler.java#KafkaReconciler
  - kind: doc
    role: context
    target: documentation/assemblies/deploying/assembly-deploy-kafka-cluster.adoc
---

# Deploy a Kafka cluster

Create a KRaft-based Kafka cluster from a `Kafka` resource and the `KafkaNodePool` resources labelled with its name. Strimzi creates the cluster's certificate authorities, runs one pod per node, exposes its listeners and, when the resource asks for them, deploys the Topic Operator, User Operator, Cruise Control and Kafka Exporter.
