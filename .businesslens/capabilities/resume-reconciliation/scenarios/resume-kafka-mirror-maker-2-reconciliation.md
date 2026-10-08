---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator removes the pause annotation from the KafkaMirrorMaker2 resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: mirror-maker-2
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
  - text: The Product reconciles the KafkaMirrorMaker2 resource again, applying changes made while it was paused
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: mirror-maker-2
        effect: changes
        from: Reconciliation paused
        to: Ready
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
---

# Resume reconciliation of a KafkaMirrorMaker2 resource

## Trigger

The Strimzi administrator has finished changing the KafkaMirrorMaker2 resource.

## Outcome

The operator acts on the KafkaMirrorMaker2 resource again.
