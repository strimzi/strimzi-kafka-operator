---
availability:
  - place: custom-resources
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaReconciler.java#manualRollingUpdate
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/AbstractConnectOperator.java#manualRollingUpdate
  - kind: doc
    role: context
    target: documentation/assemblies/deploying/assembly-rolling-updates.adoc
---

# Roll pods

Ask Strimzi to restart a Kafka node, every Kafka node of a node pool, or the workers of a Kafka Connect cluster or MirrorMaker 2, through the same safe rolling update it uses for configuration changes. The Drain Cleaner asks for the same restart when Kubernetes drains the node a Kafka pod runs on.
