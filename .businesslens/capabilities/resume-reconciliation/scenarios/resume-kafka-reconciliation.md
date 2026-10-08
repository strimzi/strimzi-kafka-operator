---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator removes the pause annotation from the Kafka resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-cluster
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The Product reconciles the Kafka resource again, applying changes made while it was paused
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-cluster
        effect: changes
        from: Reconciliation paused
        to: Ready
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-resource
---

# Resume reconciliation of a Kafka resource

## Trigger

The Strimzi administrator has finished changing the Kafka resource.

## Outcome

The operator acts on the Kafka resource again.
