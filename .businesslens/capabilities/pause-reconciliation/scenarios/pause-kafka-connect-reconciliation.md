---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the KafkaConnect resource to pause reconciliation
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connect-cluster
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-connect-resource
  - text: The Product stops acting on the KafkaConnect resource and reports reconciliation paused
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connect-cluster
        effect: changes
        from: Ready
        to: Reconciliation paused
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-connect-resource
---

# Pause reconciliation of a KafkaConnect resource

## Trigger

The Strimzi administrator needs to make fixes or several changes to the KafkaConnect resource without the operator acting on them.

## Outcome

Changes to the KafkaConnect resource are ignored until the pause is removed; what is running keeps running.
