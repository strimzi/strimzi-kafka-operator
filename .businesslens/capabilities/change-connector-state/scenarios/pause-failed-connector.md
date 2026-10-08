---
kind: validation
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator sets the requested state of a failed connector to paused
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
  - text: The connector has failed in Kafka Connect
    kind: condition
    entities:
      - entity: connector
        effect: reads
        facts:
          - Connector status
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
  - text: The Product leaves the connector failed and reports that it cannot be paused
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        from: Ready
        to: Not ready
        facts:
          - Status message
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
---

# Pause a failed connector

## Trigger

A connector failed and the Strimzi administrator tries to pause it.

## Outcome

The connector stays failed and the resource is Not ready with the reason; stopping it is still possible.
