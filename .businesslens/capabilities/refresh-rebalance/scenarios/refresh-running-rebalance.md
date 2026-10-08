---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates a rebalancing KafkaRebalance resource to refresh it
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
  - text: The Product stops the running rebalance in Cruise Control and requests a new proposal
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: changes
        from: Rebalancing
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

# Refresh a running rebalance

## Trigger

The cluster changed while replicas were moving.

## Outcome

Movement stops and a fresh proposal is ready for review.
