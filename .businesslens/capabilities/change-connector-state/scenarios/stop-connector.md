---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator sets the connector's requested state to stopped
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        facts:
          - Requested state
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
  - text: The Product stops the connector and shuts down its tasks in Kafka Connect
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        facts:
          - Connector status
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
---

# Stop a connector

## Trigger

The connector should stop and release its tasks, for example before its offsets are changed.

## Outcome

The connector is stopped with no running tasks.
