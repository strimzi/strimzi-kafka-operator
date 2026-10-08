---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the KafkaTopic resource to pause reconciliation
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
  - text: The Product stops acting on the KafkaTopic resource and reports reconciliation paused
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        from: Ready
        to: Reconciliation paused
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
---

# Pause reconciliation of a KafkaTopic resource

## Trigger

The Strimzi administrator needs to make fixes or several changes to the KafkaTopic resource without the operator acting on them.

## Outcome

Changes to the KafkaTopic resource are ignored until the pause is removed; what is running keeps running.
