---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator removes the pause annotation from the KafkaConnect resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connect-cluster
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-connect-resource
  - text: The Product reconciles the KafkaConnect resource again, applying changes made while it was paused
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connect-cluster
        effect: changes
        from: Reconciliation paused
        to: Ready
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-connect-resource
---

# Resume reconciliation of a KafkaConnect resource

## Trigger

The Strimzi administrator has finished changing the KafkaConnect resource.

## Outcome

The operator acts on the KafkaConnect resource again.
