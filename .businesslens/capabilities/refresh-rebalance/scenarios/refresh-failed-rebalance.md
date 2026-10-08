---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates a not ready KafkaRebalance resource to refresh it
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
        from: Not ready
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

# Refresh a failed rebalance

## Trigger

A proposal or rebalance failed and its cause has been fixed.

## Outcome

A fresh proposal is ready for review.
