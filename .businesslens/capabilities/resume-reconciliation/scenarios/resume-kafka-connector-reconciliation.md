---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator removes the pause annotation from the KafkaConnector resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
  - text: The Product reconciles the KafkaConnector resource again, applying changes made while it was paused
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        from: Reconciliation paused
        to: Ready
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
---

# Resume reconciliation of a KafkaConnector resource

## Trigger

The Strimzi administrator has finished changing the KafkaConnector resource.

## Outcome

The operator acts on the KafkaConnector resource again.
