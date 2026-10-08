---
kind: automatic
routes:
  api: Kubernetes API
steps:
  - text: A MirrorMaker 2 connector with auto-restart enabled, or one of its tasks, has failed
    kind: condition
    unattended: true
    entities:
      - entity: mirror-maker-2
        effect: reads
        facts:
          - Connector status
          - Mirrors
          - Auto-restart status
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
  - text: The Product restarts the connector and its failed tasks with the same back-off as other connectors
    kind: product
    entities:
      - entity: mirror-maker-2
        effect: changes
        facts:
          - Connector status
          - Auto-restart status
    contexts:
      api:
        place: custom-resources::kafka-mirror-maker-2-resource
---

# Restart a failed MirrorMaker 2 connector automatically

## Trigger

A MirrorMaker 2 connector or task fails.

## Outcome

The connector runs again.
