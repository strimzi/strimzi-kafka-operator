---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator removes the pause annotation from the KafkaRebalance resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
  - text: The Product asks Cruise Control for a fresh proposal
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: changes
        from: Reconciliation paused
        to: Pending proposal
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
---

# Resume reconciliation of a KafkaRebalance resource

## Trigger

The Strimzi administrator has finished changing the KafkaRebalance resource.

## Outcome

The operator acts on the KafkaRebalance resource again.
