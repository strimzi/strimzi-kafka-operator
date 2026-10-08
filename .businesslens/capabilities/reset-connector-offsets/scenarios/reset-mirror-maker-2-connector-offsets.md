---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the KafkaMirrorMaker2 resource to reset the offsets of one of its connectors
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: mirror-maker-2
        effect: changes
        facts:
          - Offsets request
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
  - text: The MirrorMaker 2 connector is stopped
    kind: condition
    entities:
      - entity: mirror-maker-2
        effect: reads
        facts:
          - Connector status
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
  - text: The Product resets that connector's offsets and clears the request
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: mirror-maker-2
        effect: changes
        facts:
          - Offsets
          - Offsets request
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
---

# Reset the offsets of a stopped MirrorMaker 2 connector

## Trigger

Mirroring must start over.

## Outcome

The MirrorMaker 2 connector has no committed offsets.
