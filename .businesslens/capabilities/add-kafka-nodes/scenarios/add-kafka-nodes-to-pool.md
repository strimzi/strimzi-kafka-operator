---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator raises the node pool's replicas
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
  - text: The Product starts the new Kafka nodes with the next free node IDs and adds them to the cluster
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-node
        effect: creates
        facts:
          - Node ID
          - Roles
          - Volumes
      - entity: node-pool
        effect: changes
        facts:
          - Node IDs
    contexts:
      api:
        place: custom-resources::kafka-node-pool-resource
---

# Add Kafka nodes to a node pool

## Trigger

The cluster needs more capacity.

## Outcome

The new Kafka nodes have joined the cluster.

## Edge cases

- Next node IDs on the pool choose which IDs new Kafka nodes take.
