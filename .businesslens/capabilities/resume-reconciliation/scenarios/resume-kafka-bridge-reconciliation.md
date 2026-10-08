---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator removes the pause annotation from the KafkaBridge resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: http-bridge
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-bridge-resource
  - text: The Product reconciles the KafkaBridge resource again, applying changes made while it was paused
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: http-bridge
        effect: changes
        from: Reconciliation paused
        to: Ready
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-bridge-resource
---

# Resume reconciliation of a KafkaBridge resource

## Trigger

The Strimzi administrator has finished changing the KafkaBridge resource.

## Outcome

The operator acts on the KafkaBridge resource again.
