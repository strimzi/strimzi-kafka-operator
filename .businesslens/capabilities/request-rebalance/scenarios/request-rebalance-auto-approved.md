---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates a KafkaRebalance resource with auto-approval
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: creates
        to: New
        facts:
          - Name
          - Mode
          - Goals
          - Auto-approval
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
  - text: The Product asks Cruise Control for a proposal
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: changes
        from: New
        to: Pending proposal
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
  - text: The Product receives the proposal
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
  - text: The Product starts the rebalance without waiting for approval
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: changes
        from: Proposal ready
        to: Rebalancing
        facts:
          - Progress
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

# Request a rebalance approved in advance

## Trigger

The Strimzi administrator trusts Cruise Control's proposal for this rebalance.

## Outcome

Replicas are redistributed without a separate approval.
