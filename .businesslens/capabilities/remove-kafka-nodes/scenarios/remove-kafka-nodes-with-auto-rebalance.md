---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator lowers the replicas of a node pool with the broker role
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
  - text: The Kafka cluster configures automatic rebalancing when brokers are removed
    kind: condition
    entities:
      - entity: kafka-cluster
        effect: reads
        facts:
          - Auto-rebalance on scaling
          - Cruise Control
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The Product keeps the brokers while they host replicas and creates a remove-brokers rebalance for them, approved in advance
    kind: product
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
          - Auto-approval
      - entity: kafka-cluster
        effect: changes
        facts:
          - Auto-rebalance status
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
  - text: The Product starts the rebalance
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
  - text: Cruise Control finishes moving replicas off the brokers being removed
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: changes
        from: Rebalancing
        to: Ready
        facts:
          - Progress
      - entity: kafka-node
        effect: changes
        facts:
          - Partition replicas
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
  - text: The Product deletes the finished rebalance and reports auto-rebalancing idle
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: removes
        from: Ready
      - entity: kafka-cluster
        effect: changes
        facts:
          - Auto-rebalance status
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The Product removes the now empty brokers
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-node
        effect: removes
      - entity: node-pool
        effect: changes
        facts:
          - Node IDs
    contexts:
      api:
        place: custom-resources::kafka-node-pool-resource
---

# Remove brokers with automatic rebalancing

## Trigger

The cluster needs fewer brokers and is configured to rebalance on scaling.

## Outcome

The pool runs fewer brokers and no partition replica was lost.
