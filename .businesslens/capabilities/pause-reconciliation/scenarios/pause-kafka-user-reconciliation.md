---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the KafkaUser resource to pause reconciliation
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-user
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-user-resource
  - text: The Product stops acting on the KafkaUser resource and reports reconciliation paused
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-user
        effect: changes
        from: Ready
        to: Reconciliation paused
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-user-resource
---

# Pause reconciliation of a KafkaUser resource

## Trigger

The Strimzi administrator needs to make fixes or several changes to the KafkaUser resource without the operator acting on them.

## Outcome

Changes to the KafkaUser resource are ignored until the pause is removed; what is running keeps running.
