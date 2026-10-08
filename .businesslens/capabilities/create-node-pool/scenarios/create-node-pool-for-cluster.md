---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates a KafkaNodePool resource labelled with the name of an existing cluster
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: node-pool
        effect: creates
        facts:
          - Name
          - Roles
          - Replicas
          - Storage
          - Resources
    contexts:
      api:
        place: custom-resources::kafka-node-pool-resource
  - text: The Product starts the pool's Kafka nodes with the next free node IDs
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
  - text: The Product reports the new pool as in use
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-cluster
        effect: changes
        facts:
          - Node pools in use
    contexts:
      api:
        place: custom-resources::kafka-resource
---

# Add a node pool to a cluster

## Trigger

The Strimzi administrator needs nodes with a different configuration.

## Outcome

The new Kafka nodes have joined the cluster.

## Edge cases

- Next node IDs on the new pool choose the IDs its Kafka nodes use.
- Creating a node pool does not start an automatic rebalance; new brokers receive partitions only when a rebalance moves some to them.
