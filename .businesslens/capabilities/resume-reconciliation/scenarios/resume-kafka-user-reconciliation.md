---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator removes the pause annotation from the KafkaUser resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-user
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-user-resource
  - text: The Product reconciles the KafkaUser resource again, applying changes made while it was paused
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-user
        effect: changes
        from: Reconciliation paused
        to: Ready
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-user-resource
---

# Resume reconciliation of a KafkaUser resource

## Trigger

The Strimzi administrator has finished changing the KafkaUser resource.

## Outcome

The operator acts on the KafkaUser resource again.
