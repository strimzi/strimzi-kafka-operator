---
kind: validation
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator changes the storage type or storage class, or shrinks a volume of the pool
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: node-pool
        effect: changes
        facts:
          - Storage
    contexts:
      api:
        place: custom-resources::kafka-node-pool-resource
  - text: The Product keeps the existing storage and reports a warning on the node pool and on the Kafka cluster
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: node-pool
        effect: changes
        facts:
          - Status message
      - entity: kafka-cluster
        effect: changes
        facts:
          - Status message
    contexts:
      api:
        place: custom-resources::kafka-resource
---

# Make a storage change that is not allowed

## Trigger

The Strimzi administrator asks for a storage change Strimzi does not perform.

## Outcome

Every storage change in the pool is ignored and the KafkaNodePool and Kafka resources carry a warning.
