---
kind: automatic
routes:
  api: Kubernetes API
steps:
  - text: A connector with auto-restart enabled, or one of its tasks, has failed
    kind: condition
    unattended: true
    entities:
      - entity: connector
        effect: reads
        facts:
          - Connector status
          - Auto-restart
          - Auto-restart status
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
  - text: The Product restarts the connector and its failed tasks, waiting longer after each restart, up to the maximum number of restarts
    kind: product
    entities:
      - entity: connector
        effect: changes
        facts:
          - Connector status
          - Auto-restart status
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
---

# Restart a failed connector automatically

## Trigger

A connector or task fails.

## Outcome

The connector runs again; once it has stayed healthy long enough its auto-restart count resets.

## Edge cases

- Restarts happen after 0, 2, 6, 12, 20, 30, 42 and 56 minutes and then every 60 minutes.
