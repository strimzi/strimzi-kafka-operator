---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator removes the pause annotation from the KafkaTopic resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
  - text: The Product reconciles the KafkaTopic resource again, applying changes made while it was paused
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        from: Reconciliation paused
        to: Ready
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
---

# Resume reconciliation of a KafkaTopic resource

## Trigger

The Strimzi administrator has finished changing the KafkaTopic resource.

## Outcome

The operator acts on the KafkaTopic resource again.
