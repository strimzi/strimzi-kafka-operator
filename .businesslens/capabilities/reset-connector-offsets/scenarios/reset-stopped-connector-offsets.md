---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the KafkaConnector resource to reset its offsets
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        facts:
          - Offsets request
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
  - text: The connector is stopped
    kind: condition
    entities:
      - entity: connector
        effect: reads
        facts:
          - Connector status
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
  - text: The Product resets the offsets and clears the request
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        facts:
          - Offsets
          - Offsets request
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
---

# Reset a stopped connector's offsets

## Trigger

A connector must start over.

## Outcome

The connector has no committed offsets.

## Edge cases

- A connector that is not stopped keeps its offsets and the resource carries a warning.
