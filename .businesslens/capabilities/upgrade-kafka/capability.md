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
    target: documentation/assemblies/upgrading/assembly-upgrade.adoc
---

# Upgrade Kafka

Move a Kafka cluster to a newer supported Kafka version, and afterwards to a newer KRaft metadata version.
