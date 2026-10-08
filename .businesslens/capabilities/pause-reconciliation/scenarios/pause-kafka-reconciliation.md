---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the Kafka resource to pause reconciliation
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-cluster
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The Product stops acting on the Kafka resource and reports reconciliation paused
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-cluster
        effect: changes
        from: Ready
        to: Reconciliation paused
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-resource
---

# Pause reconciliation of a Kafka resource

## Trigger

The Strimzi administrator needs to make fixes or several changes to the Kafka resource without the operator acting on them.

## Outcome

Changes to the Kafka resource are ignored until the pause is removed; what is running keeps running.
