---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator writes the offsets to apply into the alterOffsets ConfigMap of a MirrorMaker 2 connector
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: mirror-maker-2
        effect: changes
        facts:
          - Offsets
    contexts:
      api:
        place: custom-resources::connector-offsets-config-map
  - text: The Strimzi administrator annotates the KafkaMirrorMaker2 resource to alter the offsets of that connector
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
  - text: The Product applies the offsets from the ConfigMap and clears the request
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: mirror-maker-2
        effect: changes
        facts:
          - Offsets request
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
---

# Alter the offsets of a stopped MirrorMaker 2 connector

## Trigger

Mirroring must restart from different offsets.

## Outcome

The MirrorMaker 2 connector resumes from the new offsets when it runs again.
