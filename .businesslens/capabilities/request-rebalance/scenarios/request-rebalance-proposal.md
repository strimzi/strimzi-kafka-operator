---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates a KafkaRebalance resource labelled with its cluster, with its mode and goals
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: creates
        to: New
        facts:
          - Name
          - Mode
          - Brokers
          - Goals
          - Movement limits
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
  - text: Cruise Control is deployed for the Kafka cluster
    kind: condition
    entities:
      - entity: kafka-cluster
        effect: reads
        facts:
          - Cruise Control
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
  - text: The Product reports the proposal with its optimization result
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

# Request a rebalance proposal

## Trigger

The cluster's load is uneven, or brokers or volumes are being added or removed.

## Outcome

A proposal is ready for the Strimzi administrator to review.

## Edge cases

- A remove-disks rebalance names the broker volumes to empty instead of brokers.
- An add-brokers or remove-brokers rebalance without a list of brokers, or a remove-disks rebalance without volumes, is Not ready with the reason.
- Changing the KafkaRebalance resource later asks Cruise Control for a fresh proposal.
