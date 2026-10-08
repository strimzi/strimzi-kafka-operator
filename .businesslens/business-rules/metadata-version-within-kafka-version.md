---
appliesTo:
  - type: entity
    id: kafka-cluster
    effect: changes
    facts:
      - Metadata version
  - type: capability
    id: downgrade-kafka
  - type: capability
    id: upgrade-kafka
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KRaftVersionChangeCreator.java#KRaftVersionChangeCreator
---

# A Kafka cluster's metadata version never exceeds the Kafka version it runs

A requested metadata version newer than the running Kafka version is not applied; a downgrade to a Kafka version older than the metadata version in use is refused. During an upgrade the metadata version changes only after every Kafka node runs the new Kafka version.
