---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator deletes the KafkaRebalance resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: removes
        from: Ready
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
---

# Delete a rebalance

## Trigger

A rebalance has finished or is no longer wanted.

## Outcome

The resource is gone; replicas stay where they are.

## Edge cases

- Deleting a rebalance while it runs does not stop Cruise Control moving replicas; stopping it first does.
