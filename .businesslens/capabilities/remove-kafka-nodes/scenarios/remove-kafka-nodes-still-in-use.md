---
kind: validation
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
  - text: Some brokers to remove still host partition replicas
    kind: condition
    entities:
      - entity: kafka-node
        effect: reads
        facts:
          - Partition replicas
    contexts:
      api:
        place: custom-resources::kafka-node-pool-resource
  - text: The Product keeps the pool at its current size and reports the reverted scale-down on the Kafka cluster
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

# Remove brokers that still host replicas

## Trigger

The Strimzi administrator scales down before moving partitions off the brokers.

## Outcome

No Kafka node is removed and the Kafka resource carries a ScaleDownPreventionCheck warning.

## Edge cases

- With the skip broker scale-down check annotation on the Kafka resource, the brokers are removed even though they host replicas.
