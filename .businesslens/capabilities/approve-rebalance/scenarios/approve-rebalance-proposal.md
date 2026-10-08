---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the KafkaRebalance resource to approve the proposal
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
  - text: The Product starts the rebalance and clears the request
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: changes
        from: Proposal ready
        to: Rebalancing
        facts:
          - Progress
          - Rebalance request
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
  - text: The Product reports the rebalance finished
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: changes
        from: Rebalancing
        to: Ready
        facts:
          - Progress
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
---

# Approve a rebalance proposal

## Trigger

The Strimzi administrator accepts the proposal.

## Outcome

Replicas are redistributed as proposed.

## Edge cases

- A request the rebalance's current state does not accept stays on the resource with an InvalidAnnotation warning, and the state does not change.
- When Cruise Control lacks enough data to start, the rebalance returns to Pending proposal.
