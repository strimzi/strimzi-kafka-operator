---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the KafkaRebalance resource to pause reconciliation
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
  - text: The Product stops acting on the KafkaRebalance resource and reports reconciliation paused
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: changes
        from: Proposal ready
        to: Reconciliation paused
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
---

# Pause reconciliation of a KafkaRebalance resource

## Trigger

The Strimzi administrator needs to make fixes or several changes to the KafkaRebalance resource without the operator acting on them.

## Outcome

Changes to the KafkaRebalance resource are ignored until the pause is removed; what is running keeps running.
