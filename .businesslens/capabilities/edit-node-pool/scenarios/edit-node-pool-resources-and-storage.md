---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator changes the pool's resources, increases its volume sizes or adds JBOD volumes
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: node-pool
        effect: changes
        facts:
          - Resources
          - Storage
    contexts:
      api:
        place: custom-resources::kafka-node-pool-resource
  - text: The Product resizes or adds the volume claims and rolls the pool's Kafka nodes one at a time where the change needs a restart
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-node
        effect: changes
        facts:
          - Volumes
    contexts:
      api:
        place: custom-resources::kafka-node-pool-resource
---

# Change a node pool's resources or grow its storage

## Trigger

The Strimzi administrator needs more capacity on a pool's Kafka nodes.

## Outcome

The pool's Kafka nodes run with the new resources and larger volumes.

## Edge cases

- When the storage class cannot expand volumes, the Kafka resource carries a warning and the volumes keep their size.
