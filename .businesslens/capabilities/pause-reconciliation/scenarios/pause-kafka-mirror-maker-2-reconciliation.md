---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the KafkaMirrorMaker2 resource to pause reconciliation
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: mirror-maker-2
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
  - text: The Product stops acting on the KafkaMirrorMaker2 resource and reports reconciliation paused
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: mirror-maker-2
        effect: changes
        from: Ready
        to: Reconciliation paused
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
---

# Pause reconciliation of a KafkaMirrorMaker2 resource

## Trigger

The Strimzi administrator needs to make fixes or several changes to the KafkaMirrorMaker2 resource without the operator acting on them.

## Outcome

Changes to the KafkaMirrorMaker2 resource are ignored until the pause is removed; what is running keeps running.
