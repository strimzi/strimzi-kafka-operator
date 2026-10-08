---
appliesTo:
  - type: entity
    id: node-pool
    effect: changes
    facts:
      - Roles
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/KafkaClusterCreator.java#KafkaClusterCreator
  - kind: doc
    role: context
    target: documentation/modules/configuring/con-config-node-pool-roles.adoc
---

# Kafka nodes keep the broker role while they host partition replicas

Removing the broker role from a node pool whose brokers still host partition replicas is reverted with a ScaleDownPreventionCheck warning until the replicas are moved, unless the Kafka resource skips the broker scale-down check.
