---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator deletes the KafkaMirrorMaker2 resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: mirror-maker-2
        effect: removes
        from: Ready
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
  - text: The Product's workers and connectors for the deployment are removed
    kind: product
    actor: strimzi-administrator
    entities: []
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
---

# Delete MirrorMaker 2

## Trigger

Mirroring is no longer needed.

## Outcome

Nothing is mirrored any more; mirrored topics stay in the target cluster.
