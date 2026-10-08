---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the KafkaRebalance resource to refresh the proposal
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: changes
        facts:
          - Rebalance request
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
  - text: The Product requests a new proposal and clears the request
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: changes
        from: Proposal ready
        to: Pending proposal
        facts:
          - Rebalance request
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
  - text: The Product reports the new proposal
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: changes
        from: Pending proposal
        to: Proposal ready
        facts:
          - Optimization result
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
---

# Refresh a rebalance proposal

## Trigger

The proposal is out of date.

## Outcome

A fresh proposal is ready for review.

## Edge cases

- A finished rebalance can be refreshed the same way to plan another one.
