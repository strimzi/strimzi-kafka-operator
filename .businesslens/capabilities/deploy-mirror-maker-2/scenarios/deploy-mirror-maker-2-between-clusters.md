---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates a KafkaMirrorMaker2 resource with the target cluster and one mirror per source cluster
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: mirror-maker-2
        effect: creates
        to: Not ready
        facts:
          - Name
          - Replicas
          - Kafka version
          - Target cluster
          - Mirrors
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
  - text: The Product starts the workers, creates the source and checkpoint connectors and reports them ready
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: mirror-maker-2
        effect: changes
        from: Not ready
        to: Ready
        facts:
          - Connector status
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
---

# Deploy MirrorMaker 2

## Trigger

Data must be replicated between Kafka clusters.

## Outcome

Topics matching the patterns are mirrored into the target cluster.

## Edge cases

- A MirrorMaker 2 connector that fails makes the resource Not ready.
