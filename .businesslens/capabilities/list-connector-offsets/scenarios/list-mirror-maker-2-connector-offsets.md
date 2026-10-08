---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the KafkaMirrorMaker2 resource to list the offsets of one of its connectors
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
  - text: The Product writes that connector's offsets to the ConfigMap and clears the request
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
        place: custom-resources::connector-offsets-config-map
---

# List the offsets of a MirrorMaker 2 connector

## Trigger

The Strimzi administrator needs to see how far mirroring has got.

## Outcome

The ConfigMap holds the MirrorMaker 2 connector's current offsets.
