---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator lowers the node pool's replicas
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: node-pool
        effect: changes
        facts:
          - Replicas
    contexts:
      api:
        place: custom-resources::kafka-node-pool-resource
  - text: The brokers to remove host no partition replicas
    kind: condition
    entities:
      - entity: kafka-node
        effect: reads
        facts:
          - Partition replicas
    contexts:
      api:
        place: custom-resources::kafka-node-pool-resource
  - text: The Product removes the Kafka nodes, deleting their volume claims where the storage asks for it
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-node
        effect: removes
      - entity: node-pool
        effect: changes
        facts:
          - Node IDs
    contexts:
      api:
        place: custom-resources::kafka-node-pool-resource
---

# Remove empty brokers from a node pool

## Trigger

The cluster has more brokers than it needs and their partitions have been moved off.

## Outcome

The pool runs fewer Kafka nodes.

## Edge cases

- Node IDs to remove on the pool choose which Kafka nodes go first.
