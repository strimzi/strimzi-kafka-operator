---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator deletes the KafkaNodePool resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: node-pool
        effect: removes
      - entity: kafka-node
        effect: removes
        with: node-pool
    contexts:
      api:
        place: custom-resources::kafka-node-pool-resource
  - text: The Product stops the pool's pods and stops reporting the pool as in use
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

# Delete a node pool

## Trigger

The Strimzi administrator no longer needs a node pool, usually after moving its partition replicas to other brokers.

## Outcome

The pool's Kafka nodes no longer run.

## Edge cases

- Deleting a node pool does not check whether its brokers still host partition replicas, and does not start an automatic rebalance.
- The pool's volume claims are deleted with it only where its storage asks for that.
- Deleting the last node pool with the broker or the controller role leaves the Kafka resource Not ready.
