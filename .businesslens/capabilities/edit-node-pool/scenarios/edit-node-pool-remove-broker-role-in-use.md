---
kind: validation
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator removes the broker role from the pool
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: node-pool
        effect: changes
        facts:
          - Roles
    contexts:
      api:
        place: custom-resources::kafka-node-pool-resource
  - text: The pool's brokers still host partition replicas
    kind: condition
    entities:
      - entity: kafka-node
        effect: reads
        facts:
          - Partition replicas
    contexts:
      api:
        place: custom-resources::kafka-node-pool-resource
  - text: The Product keeps the broker role on the pool's brokers and reports a warning on the Kafka cluster
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-cluster
        effect: changes
        facts:
          - Status message
    contexts:
      api:
        place: custom-resources::kafka-resource
---

# Remove the broker role from Kafka nodes that hold replicas

## Trigger

The Strimzi administrator tries to turn brokers into controller-only nodes before moving their data.

## Outcome

The Kafka nodes keep the broker role until their replicas are moved, and the Kafka resource carries a warning.
