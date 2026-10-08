---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the KafkaBridge resource to pause reconciliation
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: http-bridge
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-bridge-resource
  - text: The Product stops acting on the KafkaBridge resource and reports reconciliation paused
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: http-bridge
        effect: changes
        from: Ready
        to: Reconciliation paused
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-bridge-resource
---

# Pause reconciliation of a KafkaBridge resource

## Trigger

The Strimzi administrator needs to make fixes or several changes to the KafkaBridge resource without the operator acting on them.

## Outcome

Changes to the KafkaBridge resource are ignored until the pause is removed; what is running keeps running.
