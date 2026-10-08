---
availability:
  - place: custom-resources
domain: kafka-clusters
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KRaftVersionChangeCreator.java#KRaftVersionChangeCreator
  - kind: doc
    role: context
    target: documentation/assemblies/upgrading/assembly-downgrade.adoc
---

# Downgrade Kafka

Move a Kafka cluster back to an older supported Kafka version whose metadata version is not older than the one in use.
