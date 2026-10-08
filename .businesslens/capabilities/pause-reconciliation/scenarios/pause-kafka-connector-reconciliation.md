---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the KafkaConnector resource to pause reconciliation
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
  - text: The Product stops acting on the KafkaConnector resource and reports reconciliation paused
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        from: Ready
        to: Reconciliation paused
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
---

# Pause reconciliation of a KafkaConnector resource

## Trigger

The Strimzi administrator needs to make fixes or several changes to the KafkaConnector resource without the operator acting on them.

## Outcome

Changes to the KafkaConnector resource are ignored until the pause is removed; what is running keeps running.
