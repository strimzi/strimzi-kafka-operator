---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the KafkaMirrorMaker2 resource with the connector to restart
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: mirror-maker-2
        effect: changes
        facts:
          - Connector restart request
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
  - text: The Product restarts the connector and clears the request
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: mirror-maker-2
        effect: changes
        facts:
          - Connector status
          - Connector restart request
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
---

# Restart a MirrorMaker 2 connector

## Trigger

A MirrorMaker 2 connector misbehaves.

## Outcome

The connector has restarted.

## Edge cases

- The annotation may also ask to include all or only the failed tasks.
