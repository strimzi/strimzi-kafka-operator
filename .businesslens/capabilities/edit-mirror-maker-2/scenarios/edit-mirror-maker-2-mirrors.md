---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator changes the mirrors or replicas in the KafkaMirrorMaker2 resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: mirror-maker-2
        effect: changes
        facts:
          - Mirrors
          - Replicas
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
  - text: The Product updates the connectors and workers, deletes connectors no mirror declares any more, and reports their status
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: mirror-maker-2
        effect: changes
        facts:
          - Connector status
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
---

# Change MirrorMaker 2 mirrors

## Trigger

Mirroring needs to cover different topics or sources.

## Outcome

MirrorMaker 2 mirrors as declared.

## Edge cases

- With zero replicas, no connectors are created or updated.
