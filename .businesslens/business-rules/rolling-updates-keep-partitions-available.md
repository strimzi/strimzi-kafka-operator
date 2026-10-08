---
appliesTo:
  - type: capability
    id: edit-kafka-cluster
  - type: capability
    id: upgrade-kafka
  - type: capability
    id: downgrade-kafka
  - type: capability
    id: roll-pods
  - type: capability
    id: renew-ca-certificate
  - type: capability
    id: replace-ca-key
  - type: capability
    id: add-listener
  - type: capability
    id: edit-listener
  - type: capability
    id: remove-listener
  - type: capability
    id: edit-node-pool
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/resource/KafkaRoller.java#KafkaRoller
  - kind: doc
    role: context
    target: documentation/assemblies/deploying/assembly-rolling-updates.adoc
---

# Rolling updates restart one Kafka node at a time and keep partitions available

Whenever Strimzi restarts Kafka nodes — for configuration changes, upgrades, certificate renewals or manual rolling updates — it restarts them one at a time, controllers before brokers and the active controller after the other controllers, and does not restart a broker while that would take a partition below its minimum in-sync replicas.
