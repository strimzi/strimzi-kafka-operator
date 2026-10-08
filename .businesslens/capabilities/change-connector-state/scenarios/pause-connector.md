---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator sets the connector's requested state to paused
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
  - text: The Product pauses the connector and its tasks in Kafka Connect
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

# Pause a connector

## Trigger

The connector should stop processing for a while.

## Outcome

The connector and its tasks are paused and keep their resources.
